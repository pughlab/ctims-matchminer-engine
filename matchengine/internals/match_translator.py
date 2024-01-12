from __future__ import annotations

import logging
from collections import deque
from typing import TYPE_CHECKING

import networkx as nx

from matchengine.internals.typing.matchengine_types import (
    MatchClauseData,
    ParentPath,
    MatchTree,
    NodeID,
    MatchCriteria,
    MatchCriterion,
    MultiCollectionQuery,
)
from matchengine.internals.match_query_translator import build_query_node_container
if TYPE_CHECKING:
    from typing import Generator, Dict, Any, Tuple
    from matchengine.internals.engine import MatchEngine

log = logging.getLogger("matchengine")


def extract_match_clauses_from_trial(matchengine: MatchEngine, trial) -> Generator[MatchClauseData]:
    """
    Pull out all of the matches from a trial curation.
    Return the parent path and the values of that match clause.

    Default to only extracting match clauses on steps, arms or dose levels which are open to accrual unless
    otherwise specified.
    """

    process_q = deque()
    for key, val in trial.items():

        # include top level match clauses
        if key == 'match':
            parent_path = ParentPath(tuple())
            match_clause_data = MatchClauseData(
                val,
                None,
                parent_path,
                None,
                None,
                trial[matchengine.match_criteria_transform.trial_identifier],
            )
            yield match_clause_data
        else:
            process_q.append((tuple(), key, val))

    # process nested dicts to find more match clauses
    while process_q:
        path, parent_key, parent_value = process_q.pop()
        if parent_value.__class__ is dict:
            for inner_key, inner_value in parent_value.items():
                parent_path = ParentPath(path + (parent_key, inner_key))
                if inner_key == 'match':
                    # parent_path_str is the JSON path within the trial that we are currently processing
                    parent_path_str = '.'.join(str(item) for item in parent_path)
                    is_suspended = False
                    match_level = path[-1]
                    if match_level == 'step':
                        if all(
                            [
                                arm.get('arm_suspended', 'n').lower().strip() == 'y'
                                for arm in parent_value.get('arm', list())
                            ]
                        ):
                            log.debug(f"{match_level} {parent_path_str} has no open arms")
                            is_suspended = True
                    elif match_level == 'arm':
                        if parent_value.get('arm_suspended', 'n').lower().strip() == 'y':
                            log.debug(f"{match_level} {parent_path_str} is suspended")
                            is_suspended = True
                    elif match_level == 'dose_level':
                        if parent_value.get('level_suspended', 'n').lower().strip() == 'y':
                            log.debug(f"{match_level} {parent_path_str} is suspended")
                            is_suspended = True

                    yield MatchClauseData(
                        inner_value,
                        is_suspended,
                        parent_path,
                        match_level,
                        parent_value,
                        trial[matchengine.match_criteria_transform.trial_identifier],
                    )
                else:
                    process_q.append((path + (parent_key,), inner_key, inner_value))
        elif parent_value.__class__ is list:
            for index, item in enumerate(parent_value):
                process_q.append((path + (parent_key,), index, item))


def create_match_tree(matchengine, match_clause_data: MatchClauseData) -> MatchTree:
    """
    Turn a match clause from a trial curation into a digraph.
    """
    match_clause = match_clause_data.match_clause
    process_q: deque[Tuple[NodeID, Dict[str, Any]]] = deque()
    graph = nx.DiGraph()
    node_id: NodeID = NodeID(1)
    graph.add_node(0)  # root node is 0
    graph.nodes[0]['criteria_list'] = list()
    graph.nodes[0]['is_and'] = True
    graph.nodes[0]['or_nodes'] = set()
    graph.nodes[0]['label'] = '0 - ROOT and'
    graph.nodes[0]['label_list'] = list()
    for item in match_clause:
        if any([k.startswith('or') for k in item.keys()]):
            process_q.appendleft((NodeID(0), item))
        else:
            process_q.append((NodeID(0), item))

    def graph_match_clause():
        """
        A debugging function used if the --visualize-match-paths flag is passed. This function will output images
        of the digraphs which are an intermediate data structure used to generate mongo queries later.
        """
        import matplotlib.pyplot as plt
        from networkx.drawing.nx_agraph import graphviz_layout
        import os

        labels = {node: graph.nodes[node]['label'] for node in graph.nodes}
        for node in graph.nodes:
            if graph.nodes[node]['label_list']:
                labels[node] = labels[node] + ' [' + ','.join(graph.nodes[node]['label_list']) + ']'
        pos = graphviz_layout(graph, prog="dot", root=0)
        plt.figure(figsize=(30, 30))
        nx.draw_networkx(graph, pos, with_labels=True, node_size=[600 for _ in graph.nodes], labels=labels)
        plt.savefig(
            os.path.join(
                matchengine.fig_dir,
                (
                    f'{match_clause_data.protocol_no}-'
                    f'{match_clause_data.match_clause_level}-'
                    f'{match_clause_data.internal_id}.png'
                ),
            )
        )
        return plt

    while process_q:
        parent_id, values = process_q.pop()
        parent_is_and = True if graph.nodes[parent_id].get('is_and', False) else False

        # label is 'and', 'or', 'extended_attributes' or 'clinical'
        for label, value in values.items():
            if label.startswith('and'):
                criteria_list = list()
                label_list = list()
                for item in value:
                    for inner_label, inner_value in item.items():
                        if inner_label.startswith("or"):
                            process_q.appendleft((parent_id if parent_is_and else node_id, {inner_label: inner_value}))
                        elif inner_label.startswith("and"):
                            process_q.append((parent_id if parent_is_and else node_id, {inner_label: inner_value}))
                        else:
                            criteria_list.append({inner_label: inner_value})
                            label_list.append(inner_label)
                if parent_is_and:
                    graph.nodes[parent_id]['criteria_list'].extend(criteria_list)
                    graph.nodes[parent_id]['label_list'].extend(label_list)
                else:
                    graph.add_edges_from([(parent_id, node_id)])
                    graph.nodes[node_id].update(
                        {
                            'criteria_list': criteria_list,
                            'is_and': True,
                            'is_or': False,
                            'or_nodes': set(),
                            'label': str(node_id) + ' - ' + label,
                            'label_list': label_list,
                        }
                    )
                    node_id += 1
            elif label.startswith("or"):
                or_node_id = node_id
                graph.add_node(or_node_id)
                graph.nodes[or_node_id].update(
                    {
                        'criteria_list': list(),
                        'is_and': False,
                        'is_or': True,
                        'label': str(or_node_id) + ' - ' + label,
                        'label_list': list(),
                    }
                )
                node_id += 1
                for item in value:
                    process_q.append((or_node_id, item))
                if parent_is_and:
                    parent_or_nodes = graph.nodes[parent_id]['or_nodes']
                    if not parent_or_nodes:
                        graph.add_edges_from([(parent_id, or_node_id)])
                        graph.nodes[parent_id]['or_nodes'] = {or_node_id}
                    else:
                        successors = [
                            (successor, or_node_id)
                            for parent_or_node in parent_or_nodes
                            for successor in nx.descendants(graph, parent_or_node)
                            if graph.out_degree(successor) == 0
                        ]
                        graph.add_edges_from(successors)
                else:
                    graph.add_edge(parent_id, or_node_id)
            else:
                if parent_is_and:
                    graph.nodes[parent_id]['criteria_list'].append(values)
                    graph.nodes[parent_id]['label_list'].append(label)
                else:
                    graph.add_node(node_id)
                    graph.nodes[node_id].update(
                        {
                            'criteria_list': [values],
                            'is_or': False,
                            'is_and': True,
                            'label': str(node_id) + ' - ' + label,
                            'label_list': list(),
                        }
                    )
                    graph.add_edge(parent_id, node_id)
                    node_id += 1

    if matchengine.visualize_match_paths:
        graph_match_clause()
    return MatchTree(graph)


def get_match_paths(match_tree: MatchTree) -> Generator[MatchCriterion]:
    """
    Takes a MatchTree (from create_match_tree) and yields the criteria from each possible path on the tree,
    from the root node to each leaf node
    """
    leaves = list()
    for node in match_tree.nodes:
        if match_tree.out_degree(node) == 0:
            leaves.append(node)
    for leaf in leaves:
        for path in nx.all_simple_paths(match_tree, 0, leaf) if leaf != 0 else [[leaf]]:
            match_path = []
            for depth, node in enumerate(path):
                if match_tree.nodes[node]['criteria_list']:
                    match_path.append(MatchCriteria(match_tree.nodes[node]['criteria_list'], depth, node))
            yield MatchCriterion(match_path)



def translate_match_path(
    matchengine: MatchEngine,
    match_clause_data: MatchClauseData,
    match_criterion: MatchCriterion,
) -> MultiCollectionQuery:
    """
    Translate a set of match criteria into QueryNodeContainers.
    """
    partial_qncs = []
    for node in match_criterion.criteria_list:
        for criteria in node.criteria:
            for node_name in sorted(criteria):
                node_value = criteria[node_name]
                criteria_info = []
                config = matchengine.match_criteria_transform.ctml_collection_mappings[node_name]
                if config['break_queries_into_parts']:
                    for key in sorted(node_value):
                        criteria_info.append({key: node_value[key]})
                else:
                    criteria_info.append(node_value)
                for values in criteria_info:
                    query_node_container = build_query_node_container(matchengine, node_name, node.depth, values)
                    # Filter out empty queries:
                    any_nonempty = any((not qn.is_empty()) for qn in query_node_container.query_nodes)
                    if any_nonempty:
                        partial_qncs.append(query_node_container)

    result_qncs = []
    dedup_cache = set()
    for query_node_container in partial_qncs:

        # Filter out duplicate query node containers:
        qnc_hash_key = frozenset(
            (qn.raw_collection, query_node_container.exclusion, qn.raw_query_hash()) for qn in query_node_container.query_nodes
        )
        if qnc_hash_key in dedup_cache:
            continue
        dedup_cache.add(qnc_hash_key)

        # Add to the list of query node containers:
        result_qncs.append(query_node_container)

    if not result_qncs:
        return None

    multi_collection_query = MultiCollectionQuery(result_qncs)
    return multi_collection_query
