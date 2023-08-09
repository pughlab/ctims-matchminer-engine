from __future__ import annotations

from itertools import product
from typing import TYPE_CHECKING


from matchengine.internals.typing.matchengine_types import (
    QueryNode,
    QueryTransformerResult,
    QueryPart,
    QueryNodeContainer,
)

if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine

def build_query_node_container(
    matchengine: MatchEngine,
    node_name: str,
    node_depth: int,
    values: dict,
):
    """
    Build a QueryNodeContainer from a node_name (e.g. clinical, genomic) and
    a set of key/value (values).
    """
    # Convert the key-value pairs in CTML into QueryNodes
    # A QueryNodeContainer is an OR of QueryNode; a QueryNode is an AND of QueryParts,
    # possibly negated by the .exclusion field. (The "OR" feature currently only works for non-clinical
    # collections, since we only use this feature for SVs.)
    mct = matchengine.match_criteria_transform
    mappings = mct.ctml_collection_mappings[node_name]
    trial_key_mappings = {k.lower(): v for k, v in mappings['trial_key_mappings'].items()}

    # Generate query parts; each key/value pair maps to an OR of query parts:
    query_parts_lists = []
    for trial_key in sorted(values.keys()):

        trial_key_settings = trial_key_mappings.get(trial_key.lower(), dict())
        if trial_key_settings.get('ignore', False):
            continue

        trial_value = values[trial_key]

        sample_value_function_name = trial_key_settings.get('sample_value', 'nomap')
        sample_function = mct.query_transformers.get(sample_value_function_name)
        if not sample_function:
            raise ValueError(f"Mapper {sample_value_function_name!r} not found")

        sample_function_args = dict(
            sample_key=trial_key,
            trial_value=trial_value,
            current_date=matchengine.age_comparison_date,
        )
        sample_function_args.update(trial_key_settings)
        result: QueryTransformerResult = sample_function(**sample_function_args)

        query_parts = []
        for query_clause, negate in result.results:
            part = QueryPart(
                query=query_clause,
                negate=negate,
                render=True,
            )
            query_parts.append(part)
        query_parts_lists.append(query_parts)

    # Generate query nodes, combining the ORs above by taking the cartesian product:
    query_nodes = []
    exclusions = []
    for query_part_group in product(*query_parts_lists):
        parts = [
            query_part.__copy__()
            for query_part in query_part_group
        ]
        exclusion = any(qp.negate for qp in parts)
        node = QueryNode(
            query_parts=parts,
            raw_collection=mappings["query_collection"],
            raw_join_field=mappings["join_field"],
            raw_id_field=mappings["id_field"]
        )
        exclusions.append(exclusion)
        query_nodes.append(node)

    all_exclusions = all((e for e in exclusions))
    all_inclusions = all(((not e) for e in exclusions))
    if (not all_exclusions) and (not all_inclusions):
        raise ValueError("Single criteria must be either exclusion or inclusion")
    # Build the query node container, an "and" of the query nodes:
    query_node_container = QueryNodeContainer(
        query_nodes=query_nodes,
        trial_key=node_name,
        trial_value=values,
        node_depth=node_depth,
        exclusion=exclusion
    )
    matchengine.query_processor.transform(query_node_container)
    for query_node in query_node_container.query_nodes:
        query_node.finalize()
    return query_node_container
