import datetime
import json
import os
from unittest import TestCase
from datetime import date
from bson import ObjectId

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.engine import MatchEngine


class RunLogTest(TestCase):
    """
    The run_log is a log which keeps track of protocols and sample ID's used by the engine
    in previous runs, by protocol. There are three sources used to determine if any trial and/or
    sample should be updated during any given matchengine run. Those sources are the:
        (1) run_log,
        (2) trial _updated fields
        (3) clinical _updated fields,

    Running and updating only the necessary trials and patients is the default behavior of the
    matchengine unless otherwise specified through a CLI flag. These tests enumerate many
    possible combinations of trial and/or patient data changes, and the subsequent expected states
    of the trial_match collection as the matchengine is run on changing and updated data.

    It is assumed that if a patient's extended_attributes document is updated or added, the corresponding
    clinical document's _updated date is updated as well.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.start_time = datetime.datetime(2000, 7, 12, 9, 47, 40)
        with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
            setup_db.trial.drop()
            setup_db.trial_match.drop()
            setup_db.run_log_trial_match.drop()
            setup_db.clinical.update_many(
                {},
                {"$set": { "_updated": self.start_time }}
            )
            setup_db.clinical.update_many(
                {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                {
                    "$set": {
                        "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                        "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1),
                    }
                },
            )
            setup_db.clinical.update_many(
                {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                {"$set": {"VITAL_STATUS": "alive", "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}},
            )
        self.me = None

    def tearDown(self):
        if self.me:
            self.me.__exit__(None, None, None)
            self.me = None
        with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
            setup_db.clinical.update_many(
                {},
                {"$set": {"_updated": None }}
            )

    def _reset(self, **kwargs):
        trials_to_load = kwargs.pop('trials_to_load', [])
        if trials_to_load:
            trials_to_load = [
                os.path.join('matchengine', 'tests', 'data', 'integration_trials', t + '.json') for t in trials_to_load
            ]
            with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
                for trial_path in trials_to_load:
                    with open(trial_path) as trial_file_handle:
                        trial = json.load(trial_file_handle)
                    trial['_updated'] = self.start_time
                    setup_db.trial.insert_one(trial)

        age_comparison_date = kwargs.pop("age_comparison_date", date(2000, 7, 12))
        start_time_utc = kwargs.pop("start_time_utc", self.start_time)

        if self.me:
            self.me.__exit__(None, None, None)
            self.me = None

        self.start_time += datetime.timedelta(minutes=7)
        self.me = MatchEngine(**kwargs, age_comparison_date=age_comparison_date, start_time_utc=start_time_utc)
        self.start_time += datetime.timedelta(minutes=5)

        assert self.me.db_rw.name == 'integration'

    def test_updated_sample_updated_trial_new_match_hashes(self):
        """
        Updated sample, updated curation, trial matches before, trial matches after, but different hashes
        :return:
        """

        self._reset(
            trials_to_load=['run_log_arm_closed'],
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish", "_updated": self.start_time}},
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                    "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1),
                }
            },
        )
        self.me.db_rw.trial.delete_many({})

        self._reset(
            trials_to_load=["run_log_arm_open"],
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Lung Adenocarcinoma", "_updated": self.start_time}},
        )

        self._reset(
            trials_to_load=[],
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 4
        assert len(disabled_trial_matches) == 1
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

    def test_updated_sample_updated_trial_new_matches(self):
        """
        Updated sample, updated curation, trial matches after, but not before
        :return:
        """

        self._reset(
            trials_to_load=['run_log_arm_closed'],
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                    "_updated": datetime.datetime(1990, 1, 1, 1, 1, 1),
                }
            },
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        assert len(trial_matches) == 0
        assert len(run_log_trial_match) == 1
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Medullary Carcinoma of the Colon",
                    "_updated": self.start_time,
                }
            },
        )
        self.me.db_rw.trial.delete_many({})
        self._reset(
            trials_to_load=["run_log_arm_open"],
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 2
        assert len(run_log_trial_match) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                    "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

    def test_match_on_closed(self):
        self._reset(
            trials_to_load=['all_closed_trial_closed']
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 0
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 0

        self._reset(match_on_closed=True)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 8
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 8

        self._reset(match_on_closed=False)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 8
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 0

    def test_match_on_deceased(self):
        self._reset(
            trials_to_load=['run_log_arm_open']
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 3
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 3

        self._reset(match_on_deceased=True)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 5
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 5

        self._reset(match_on_deceased=False)
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert self.me.db_ro.trial_match.count_documents({}) == 5
        assert self.me.db_ro.trial_match.count_documents({ 'is_disabled': False }) == 3

    def test_sample_update_new_match(self):
        """
        Updated sample leads to new trial match
        Existing sample not updated does not cause new trial matches
        Sample that doesn't match never matches
        :return:
        """

        self._reset(
            trials_to_load=['run_log_arm_closed'],
            match_on_closed=True,
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish", "_updated": datetime.datetime.now()}},
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(trial_matches) == 2
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                    "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1),
                }
            },
        )
        self._reset(
            match_on_closed=True,
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

        self._reset(
            match_on_closed=True,
        )
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                    "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 1
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

    def test_trial_update_sample_update_no_changes_1(self):
        """
        Update a trial field not used in matching.
        Samples who have matches should continue to have matches.
        Samples without matches should still not have matches.
        :return:
        """
        self._reset(
            trials_to_load=['run_log_arm_open'],
            match_on_closed=True,
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(non_match) == 0

        self._reset(
            match_on_closed=True,
        )

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-007"},
            {"$set": {"unused_field": "ricky_bobby", "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}},
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(non_match) == 0

    def test_trial_update_and_sample_update_no_changes(self):
        """
        Update a trial arm status field. Update a sample.
        After update sample with matches should continue to have matches.
        After update sample without matches should still not have matches.
        :return:
        """
        self._reset(
            trials_to_load=['run_log_two_arms'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        for match in trial_matches:
            assert match['internal_id'] == 101
            assert match['is_disabled'] == False
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(non_match) == 0

        self._reset()

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-007"},
            {
                "$set": {
                    "treatment_list.step.0.arm.1.arm_suspended": "N",
                    "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                }
            },
        )
        # update non-match
        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799df6756630d8dd068bb"},
            {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish", "_updated": datetime.datetime.now()}},
        )

        # update matching
        self.me.db_rw.genomic.insert_one(
            {
                "SAMPLE_ID": "5d2799da6756630d8dd066a6",
                "clinical_id": ObjectId("5d2799da6756630d8dd066a6"),
                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                "TRUE_HUGO_SYMBOL": "sonic_the_hedgehog",
            }
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        for match in trial_matches:
            assert match['internal_id'] == 101
            assert match['is_disabled'] == False
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(non_match) == 0

        self.me.db_rw.genomic.delete_many({"TRUE_HUGO_SYMBOL": "sonic_the_hedgehog"})

    def test_age_in_out(self):
        self.start_time += datetime.timedelta(days=36500)
        self._reset(trials_to_load=['run_log_two_arms'], age_comparison_date=date(1972, 1, 1))
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        matches = list(self.me.db_ro.trial_match.find({'is_disabled': False, 'sample_id': '5d2799da6756630d8dd066a6'}))
        self.assertEqual(matches, [])

        self._reset(age_comparison_date=date(1974, 1, 1))
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        matches = list(self.me.db_ro.trial_match.find({'is_disabled': False, 'sample_id': '5d2799da6756630d8dd066a6'}))
        self.assertEqual(len(matches), 2)

    def test_trial_update_and_patient_expiring_1(self):
        """
        Update a trial arm status field.
        Update a sample's vital_status to deceased.
        Sample should no longer have matches.
        :return:
        """
        self._reset(
            trials_to_load=['run_log_two_arms'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
            {
                "$set": {
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                    "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self._reset()

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-007"},
            {
                "$set": {
                    "treatment_list.step.0.arm.1.arm_suspended": "N",
                    "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "deceased", "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}},
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "alive", "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}},
        )

        self._reset()

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-007"},
            {"$set": {"unused_field": "ricky_bobby", "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}},
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 3

    def test_trial_update_and_patient_expiring(self):
        """
        Update a trial curation.
        Update a sample's vital_status to deceased.
        Sample should no longer have matches.
        :return:
        """
        self._reset(
            trials_to_load=['run_log_two_arms'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self._reset()

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-007"},
            {
                "$set": {
                    "treatment_list.step.0.arm.0.match.0.and.0.hugo_symbol": "BRAF",
                    "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "deceased", "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}},
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 5
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

    def test_patient_expiring(self):
        """
        Update a sample's vital_status to deceased.
        Sample should have matches before run and not after.
        :return:
        """
        self._reset(
            trials_to_load=['run_log_arm_open'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        sample_count = 0
        for match in enabled_trial_matches:
            if match['sample_id'] == "5d2799da6756630d8dd066a6":
                sample_count += 1
        assert sample_count == 2
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "deceased", "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}},
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 1
        for match in enabled_trial_matches:
            assert match['sample_id'] != "5d2799da6756630d8dd066a6"
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "alive", "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}},
        )

    def test_trial_arm_opening(self):
        """
        Update a trial arm status to open.
        Run on a new sample.
        Sample should have matches.
        Sample which doesn't match should still not match
        :return:
        """
        self._reset(
            trials_to_load=['all_closed'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        known_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799cc6756630d8dd06265"}))
        assert len(enabled_trial_matches) == 0
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(no_match) == 0
        assert len(known_match) == 0

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-001"},
            {
                "$set": {
                    "treatment_list.step.0.arm.0.arm_suspended": "N",
                    "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
                }
            },
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        known_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799cc6756630d8dd06265"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(no_match) == 0
        assert len(known_match) == 1

    def test_new_sample_matches(self):
        """
        Update a trial field not used in matching.
        Run on a new sample.
        Sample should have matches.
        Sample which doesn't match should still not match.
        :return:
        """
        self._reset(
            trials_to_load=['all_open'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(no_match) == 0

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-001"},
            {"$set": {"unused_field": "ricky_bobby", "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}},
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(no_match) == 0

    def test_open_arm_closing(self):
        """
        Run engine on open trials and find matches
        Close trial
        Run engine again
        Trial matches should be disabled
        :return:
        """
        self._reset(
            trials_to_load=['run_log_arm_open'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))

        assert len(enabled_trial_matches) == 3
        assert len(disabled_trial_matches) == 0

        self.me.db_rw.trial.update_many(
            {"protocol_no": "10-002"},
            {
                "$set": {
                    "status": "closed to accrual",
                    "_summary.status": [{'value': 'closed to accrual'}],
                    "_updated": self.start_time,
                }
            },
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))

        assert len(enabled_trial_matches) == 0
        assert len(disabled_trial_matches) == 3

    def test_patient_expiring_no_matches(self):
        """
        Update a sample's vital_status to deceased.
        Sample should not have matches before or after run.
        A third run with no trial changes should not produce matches.
        :return:
        """
        self._reset(
            trials_to_load=['all_closed'],
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update_many(
            {"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
            {"$set": {"VITAL_STATUS": "deceased", "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}},
        )

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 2

        self._reset()

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 3
