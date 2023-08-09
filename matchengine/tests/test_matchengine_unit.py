import glob
import json
import os
from unittest import TestCase

from matchengine.internals.engine import MatchEngine
from matchengine.internals.match_translator import create_match_tree, get_match_paths, extract_match_clauses_from_trial, \
    translate_match_path
from matchengine.internals.typing.matchengine_types import MatchClause, MatchCriteria, MatchCriterion
from matchengine.internals.typing.matchengine_types import MatchClauseData, ParentPath, MatchClauseLevel
from matchengine.internals.utilities.object_comparison import nested_object_hash
from matchengine.plugin_stub import QueryTransformers


class TestMatchEngine(TestCase):

    def setUp(self) -> None:
        """init matchengine without running __init__ since tests will need to instantiate various values individually"""
        self.me = MatchEngine(
            plugin_dir = 'matchengine/tests/plugins',
            visualize_match_paths = False
        )

        with open('matchengine/tests/config.json') as config_file_handle:
            self.config = json.load(config_file_handle)

    def tearDown(self):
        self.me.__exit__(None, None, None)

    def test_query_transform(self):

        is_negate = QueryTransformers()._is_negate

        assert is_negate('this') == ('this', False)
        assert is_negate('!this') == ('this', True)
        assert is_negate('!') == (str(), True)
        assert is_negate('') == (str(), False)

        transform_args = {
            'trial_path': 'test',
            'trial_key': 'test',
            'trial_value': 'test',
            'sample_key': 'test',
            'file': 'external_file_mapping_test.json'
        }

        query_transform_result = self.me.match_criteria_transform.query_transformers['nomap'](**transform_args).results[0]
        nomap_ret, nomap_no_negate = query_transform_result
        assert len(nomap_ret) == 1 and nomap_ret['test'] == 'test' and not nomap_no_negate

    def test_extract_match_clauses_from_trial(self):
        self.me.trials = dict()
        self.me.match_on_closed = False
        with open('./matchengine/tests/data/trials/11-111.json') as f:
            data = json.load(f)
            match_clause = data['treatment_list']['step'][0]['arm'][0]['match'][0]
            self.me.trials['11-111'] = data

        extracted = next(extract_match_clauses_from_trial(self.me, self.me.trials['11-111']))
        assert extracted.match_clause[0]['and'] == match_clause['and']
        assert extracted.parent_path == ('treatment_list', 'step', 0, 'arm', 0, 'match')
        assert extracted.match_clause_level == 'arm'

    def test_create_match_tree(self):
        self.me.trials = dict()
        for file in glob.glob('./matchengine/tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial

        with open('./matchengine/tests/data/create_match_tree_expected.json') as f:
            test_cases = json.load(f)

        for trial in self.me.trials:
            me_trial = self.me.trials[trial]
            match_tree = create_match_tree(self.me, MatchClauseData(match_clause=me_trial,
                                                                    parent_path=ParentPath(()),
                                                                    match_clause_level=MatchClauseLevel('arm'),
                                                                    match_clause_additional_attributes={},
                                                                    protocol_no='12-345',
                                                                    is_suspended=True))
            test_case = test_cases[os.path.basename(trial)]
            assert len(test_case["nodes"]) == len(match_tree.nodes)
            for test_case_key in test_case.keys():
                if test_case_key == "nodes":
                    for node_id, node_attrs in test_case[test_case_key].items():
                        graph_node = match_tree.nodes[int(node_id)]
                        assert len(node_attrs) == len(graph_node)
                        assert nested_object_hash(node_attrs) == nested_object_hash(graph_node)
                else:
                    for test_item, graph_item in zip(test_case[test_case_key], getattr(match_tree, test_case_key)):
                        for idx, test_item_part in enumerate(test_item):
                            assert test_item_part == graph_item[idx]

    def test_get_match_paths(self):
        self.me.trials = dict()
        for file in glob.glob('./matchengine/tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial
        with open("./matchengine/tests/data/get_match_paths_expected.json") as f:
            test_cases = json.load(f)
        for trial in self.me.trials:
            filename = os.path.basename(trial)
            me_trial = self.me.trials[trial]
            match_tree = create_match_tree(self.me, MatchClauseData(match_clause=me_trial,
                                                                    parent_path=ParentPath(()),
                                                                    match_clause_level=MatchClauseLevel('arm'),
                                                                    match_clause_additional_attributes={},
                                                                    is_suspended=True,
                                                                    protocol_no='12-345'))
            match_paths = list(get_match_paths(match_tree))
            for test_case, match_path in zip(test_cases[filename], match_paths):
                for test_case_criteria_idx, test_case_criteria in enumerate(test_case["criteria_list"]):
                    match_path_criteria = match_path.criteria_list[test_case_criteria_idx]
                    assert test_case_criteria["depth"] == match_path_criteria.depth
                    for inner_test_case_criteria, inner_match_path_criteria in zip(test_case_criteria["criteria"],
                                                                                   match_path_criteria.criteria):
                        assert nested_object_hash(inner_test_case_criteria) == nested_object_hash(
                            inner_match_path_criteria)

    def test_translate_match_path(self):
        self.me.trials = dict()
        match_clause_data = MatchClauseData(match_clause=MatchClause([{}]),
                                            parent_path=ParentPath(()),
                                            match_clause_level=MatchClauseLevel('arm'),
                                            match_clause_additional_attributes={},
                                            protocol_no='12-345',
                                            is_suspended=True)
        match_paths = translate_match_path(
            self.me,
            match_clause_data=match_clause_data,
            match_criterion=MatchCriterion([MatchCriteria({}, 0, 0)]
        ))
        assert match_paths is None

    def test_comparable_dict(self):
        assert nested_object_hash({}) == nested_object_hash({})
        assert nested_object_hash({"1": "1",
                                   "2": "2"}) == nested_object_hash({"2": "2",
                                                                     "1": "1"})
        assert nested_object_hash({"1": [{}, {2: 3}],
                                   "2": "2"}) == nested_object_hash({"2": "2",
                                                                     "1": [{2: 3}, {}]})
        assert nested_object_hash({"1": [{'set': {1, 2, 3}}, {2: 3}],
                                   "2": "2"}) == nested_object_hash({"2": "2",
                                                                     "1": [{2: 3}, {'set': {3, 1, 2}}]})
        assert nested_object_hash({
            1: {
                2: [
                    {
                        3: 4,
                        5: {6, 7}
                    }
                ]
            },
            "4": [9, 8]
        }) != nested_object_hash({
            1: {
                2: [
                    {
                        3: 4,
                        9: {6, 7}
                    }
                ]
            },
            "4": [9, 8]
        })
