import csv
import datetime
import json
import os
from shutil import which
from datetime import date
from collections import defaultdict
from unittest import TestCase, SkipTest

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.engine import MatchEngine

import warnings

try:
    __import__('pandas')
except ImportError:
    pass

class IntegrationTestMatchengine(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_run_done = False

    def _reset(self, **kwargs):
        with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
            if not self.first_run_done:
                self.first_run_done = True

            assert setup_db.name == 'integration'

            if not kwargs.get("skip_sample_id_reset", False):
                setup_db.clinical.update_many({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                         {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                                                   "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}})

            if not kwargs.get("skip_vital_status_reset", False):
                setup_db.clinical.update_many({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                         {"$set": {"VITAL_STATUS": "alive",
                                                   "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}})

            if kwargs.get('do_reset_trial_matches', False):
                setup_db.trial_match.drop()

            if kwargs.get('reset_run_log', False):
                setup_db.run_log_trial_match.drop()

            if kwargs.get('do_reset_trials', False):
                setup_db.trial.drop()
                trials_to_load = map(lambda x: os.path.join('matchengine',
                                                            'tests',
                                                            'data',
                                                            'integration_trials',
                                                            x + '.json'),
                                     kwargs.get('trials_to_load', list()))
                for trial_path in trials_to_load:
                    with open(trial_path) as trial_file_handle:
                        trial = json.load(trial_file_handle)
                    setup_db.trial.insert_one(trial)
            if kwargs.get('do_rm_clinical_run_history', False):
                setup_db.clinical_run_history_trial_match.drop()

        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)

        self.me = MatchEngine(
            match_on_deceased=kwargs.get('match_on_deceased', True),
            match_on_closed=kwargs.get('match_on_closed', True),
            num_workers=kwargs.get('num_workers', 1),
            visualize_match_paths=kwargs.get('visualize_match_paths', False),
            fig_dir=kwargs.get('fig_dir', '/tmp/'),
            protocol_nos=kwargs.get('protocol_nos', None),
            sample_ids=kwargs.get('sample_ids', None),
            age_comparison_date=kwargs.get("age_comparison_date", date(2000,7,12))
        )

        assert self.me.db_rw.name == 'integration'

    def test__match_on_deceased_match_on_closed(self):
        self._reset(do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me._matches['10-001']) == 5
        assert len(self.me._matches['10-002']) == 5
        assert len(self.me._matches['10-003']) == 5
        assert len(self.me._matches['10-004']) == 5

    def test__match_on_deceased(self):
        self._reset(match_on_deceased=True,
                    match_on_closed=False,
                    reset_run_log=True,
                    skip_sample_id_reset=False,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-002', '10-003', '10-004'})) == 3
        assert len(self.me._matches['10-002']) == 5
        assert len(self.me._matches['10-003']) == 5


    def test_duplicate_hashes(self):
        self._reset(reset_run_log=True,
                    skip_sample_id_reset=False,
                    do_reset_trials=True,
                    do_reset_trial_matches=True,
                    trials_to_load=['all_open_dup'])
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_rw['trial_match'].count_documents({}) == 35
        assert self.me.db_rw['trial_match'].count_documents({'is_disabled': False}) == 35
        assert len(self.me.db_rw['trial_match'].distinct('hash')) == 24
        for doc in list(self.me.db_rw['trial_match'].find({})):
            doc = dict(doc)
            del doc['_id']
            self.me.db_rw['trial_match'].insert_one(doc)
            del doc['_id']
            self.me.db_rw['trial_match'].insert_one(doc)
        assert self.me.db_rw['trial_match'].count_documents({}) == 35 * 3
        assert self.me.db_rw['trial_match'].count_documents({'is_disabled': False}) == 35 * 3
        self._reset(reset_run_log=True)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_rw['trial_match'].count_documents({}) == 35 * 3
        assert self.me.db_rw['trial_match'].count_documents({'is_disabled': False}) == 35

        clinical_ids = sorted(self.me.db_rw['trial_match'].distinct('clinical_id'))
        found_hashes = set()
        for doc in self.me.db_rw['trial_match'].find({ 'clinical_id': { '$in': clinical_ids[:5] } }):
            doc = dict(doc)
            if not doc['is_disabled']:
                if doc['hash'] not in found_hashes:
                    found_hashes.add(doc['hash'])
                else:
                    self.me.db_rw['trial_match'].delete_one({'_id': doc['_id']})

        found_hashes = set()
        for doc in self.me.db_rw['trial_match'].find({ 'clinical_id': { '$in': clinical_ids[-5:] } }):
            doc = dict(doc)
            if doc['is_disabled']:
                if doc['hash'] not in found_hashes:
                    found_hashes.add(doc['hash'])
                else:
                    self.me.db_rw['trial_match'].delete_one({'_id': doc['_id']})

        assert self.me.db_rw['trial_match'].count_documents({}) == 79
        assert self.me.db_rw['trial_match'].count_documents({'is_disabled': False}) == 30
        old_ids = { x['_id'] for x in self.me.db_rw['trial_match'].find({ 'is_disabled': False }) }
        self._reset(reset_run_log=True)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_rw['trial_match'].count_documents({}) == 79
        assert self.me.db_rw['trial_match'].count_documents({'is_disabled': False}) == 35
        new_ids = { x['_id'] for x in self.me.db_rw['trial_match'].find({ 'is_disabled': False }) }
        assert old_ids.issubset(new_ids)


    def test__match_on_closed(self):
        self._reset(match_on_deceased=False,
                    match_on_closed=True,
                    reset_run_log=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me._matches['10-001']) == 4
        assert len(self.me._matches['10-002']) == 4
        assert len(self.me._matches['10-003']) == 4
        assert len(self.me._matches['10-004']) == 4

    def test_update_trial_matches(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        for protocol_no in self.me.trials.keys():
            self.me.update_matches_for_protocol_number(protocol_no)
        assert self.me.db_ro.trial_match.count_documents({}) == 48

    def test_wildcard_protein_change(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['wildcard_protein_found', 'wildcard_protein_not_found'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['10-005']) == 64

    def test_match_on_individual_protocol_no(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['wildcard_protein_not_found'],
                    protocol_nos={'10-006'})
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches.keys()) == 1

    def test_match_on_individual_sample(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            reset_run_log=True,
            trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'],
            sample_ids={'5d2799cb6756630d8dd0621d'}
        )
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['10-001']) == 1
        assert len(self.me._matches['10-002']) == 1
        assert len(self.me._matches['10-003']) == 1
        assert len(self.me._matches['10-004']) == 1

    def test_output_csv(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        filename = f'trial_matches_{datetime.datetime.now().strftime("%b_%d_%Y_%H:%M")}.csv'
        try:
            self.me.create_output_csv()
            assert os.path.exists(filename)
            assert os.path.isfile(filename)
            with open(filename) as csv_file_handle:
                csv_reader = csv.DictReader(csv_file_handle)
                rows = list(csv_reader)
            assert sum([1
                        for protocol_matches in self.me._matches.values()
                        for sample_matches in protocol_matches.values()
                        for _ in sample_matches]) == 48
            assert len(rows) == 48
            os.unlink(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.unlink(filename)
            raise e

    def test_massive_match_clause(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['massive_match_clause'],
                    match_on_deceased=True,
                    match_on_closed=True,
                    reset_run_log=True,
                    num_workers=1)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        print(len(self.me._matches["11-113"]))

    def test_context_handler(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            reset_run_log=True,
            trials_to_load=['all_closed']
        )
        assert self.me.db_rw.name == 'integration'
        with MatchEngine(sample_ids={'5d2799cb6756630d8dd0621d'},
                         protocol_nos={'10-001'},
                         match_on_closed=True,
                         match_on_deceased=True,
                         num_workers=1) as me:
            me.get_matches_for_trial('10-001')
            assert not me._loop.is_closed()
        assert me._loop.is_closed()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                me.get_matches_for_trial('10-001')
                raise AssertionError("MatchEngine should have failed")
            except RuntimeError as e:
                print(f"Found expected RuntimeError {e}")

    def test_signatures(self):
        self._reset(do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['signatures'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['99-9999']['5d2799df6756630d8dd068ca']) == 5

    def test_tmb(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['tmb'],
                    reset_run_log=True)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['99-9999']['1d2799df4446699a8ddeeee']) == 4
        assert len(self.me._matches['99-9999']['4d2799df4446630a8dd068dd']) == 3
        assert len(self.me._matches['99-9999']['1d2799df4446699a8dd068ee']) == 4

    def test_unstructured_sv(self):
        self._reset(do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['unstructured_sv'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        matches = self.me._matches['10-005']['1d2799df4446699a8ddeeee']
        assert matches[0]['genomic_alteration'] == 'EGFR Structural Variation'
        assert len(matches) == 1

    def test_structured_sv(self):
        self._reset(do_reset_trials=True,
                    reset_run_log=True,
                    trials_to_load=['structured_sv'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert '5d2799df6756630d8dd068c6' in self.me.matches['99-9999']
        assert len(self.me.matches['99-9999']['5d2799df6756630d8dd068c6']) == 44
        caught_matches = defaultdict(int)
        for match in self.me.matches['99-9999']['5d2799df6756630d8dd068c6']:
            alteration = match.get('genomic_alteration')
            if match['reason_type'] == 'genomic':
                if match['internal_id'] == 1234566:
                    assert (alteration
                            not in
                            {
                                "CLIP4-ALK Structural Variation",
                                "ALK-CLIP4 Structural Variation",
                                "EML4-EML4 Structural Variation"
                            })
                else:
                    caught_matches[alteration] += 1
        check_against = {
            '!TP53 Structural Variation': 12,
            'TFG-ALK Structural Variation': 2,
            'ALK-TFG Structural Variation': 2,
            'STRN-intergenic Structural Variation': 2,
            'RANDB2-ALK Structural Variation': 2,
            'ALK-RANDB2 Structural Variation': 2,
            'NPM1-intergenic Structural Variation': 6,
            'KIF5B-ALK Structural Variation': 2,
            'ALK-KIF5B Structural Variation': 2,
            'CLIP4-ALK Structural Variation': 1,
            'this should only match to any_gene-KRAS Structural Variation': 3,
            'KRAS-this should only match to any_gene Structural Variation': 3,
            'EML4-EML4 Structural Variation': 3,
            'this should only match to any_gene-this should only match to any gene Structural Variation': 1,
            'ALK-CLIP4 Structural Variation': 1
        }
        for alteration, count in caught_matches.items():
            assert check_against[alteration] == count

    def changed_deceased_flag_fail(self):
        """
        The matchengine should always run with the same deceased flag in order
        to guarantee data integrity
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            do_rm_clinical_run_history=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            skip_vital_status_reset=False,
        )
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()

        with self.assertRaises(SystemExit) as cm:
            self._reset(
                do_reset_trial_matches=False,
                do_reset_trials=True,
                trials_to_load=["run_log_arm_open"],
                reset_run_log=False,
                match_on_closed=False,
                match_on_deceased=True,
                do_rm_clinical_run_history=False,
                do_reset_time=False,
                skip_sample_id_reset=False
            )
        self.assertEqual(cm.exception.code, 1)
        self.me.__exit__(None, None, None)

    def changed_match_on_closed_to_open_fail(self):
        """
        The matchengine should always run with the same trial open/closed flag
        in order to guarantee data integrity
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            do_rm_clinical_run_history=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            skip_vital_status_reset=False,
        )
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()

        with self.assertRaises(SystemExit) as cm:
            self._reset(
                do_reset_trial_matches=False,
                do_reset_trials=True,
                trials_to_load=["run_log_arm_open"],
                reset_run_log=False,
                match_on_closed=True,
                match_on_deceased=False,
                do_rm_clinical_run_history=False,
                do_reset_time=False,
                skip_sample_id_reset=False
            )
        self.assertEqual(cm.exception.code, 1)
        self.me.__exit__(None, None, None)

    def tearDown(self) -> None:
        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)
