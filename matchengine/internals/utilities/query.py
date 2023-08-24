from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict

from matchengine.internals.typing.matchengine_types import MatchReason, MongoQuery

if TYPE_CHECKING:
    from bson import ObjectId
    from matchengine.internals.engine import MatchEngine
    from matchengine.internals.typing.matchengine_types import (
        ClinicalID,
        MultiCollectionQuery,
        QueryNodeContainer,
        QueryNode,
    )
    from typing import (
        Tuple,
        Set,
        List,
    )

log = logging.getLogger('matchengine.query')


def _get_query_order(matchengine: MatchEngine, qnc: QueryNodeContainer):
    """
    Get a sort order for QueryNodeContainers. This tries to optimize things slightly
    by following the user-specified join_order and running exclusion criteria last.
    """
    mappings = matchengine.match_criteria_transform.ctml_collection_mappings
    join_order = mappings[qnc.trial_key].get('join_order') or 100
    hashes = tuple(sorted([qn.raw_query_hash() for qn in qnc.query_nodes]))
    return (join_order, qnc.exclusion, hashes)


def _get_cache_key(query_node: QueryNode):
    return (
        query_node.raw_collection,
        query_node.raw_join_field,
        query_node.raw_id_field,
        query_node.raw_query_hash(),
    )


async def run_query_node(
    matchengine: MatchEngine,
    query_node: QueryNode,
    clinical_ids: Set[ClinicalID],
) -> Set[ClinicalID]:
    if not clinical_ids:
        return {}

    cache_key = _get_cache_key(query_node)
    id_cache = matchengine.cache.query_results.setdefault(cache_key, {})


    future_map = matchengine.cache.query_tasks.setdefault(cache_key, {})
    to_wait = set()
    need_new = set()
    for cid in clinical_ids:
        future = future_map.get(cid, None)
        if future is None:
            need_new.add(cid)
        elif not future.done():
            to_wait.add(future)

    if need_new:
        collection = query_node.raw_collection
        join_field = query_node.raw_join_field
        id_field = query_node.raw_id_field

        group_future = matchengine._loop.create_future()
        query = query_node.extract_raw_query()
        for cid in need_new:
            future_map[cid] = group_future
        new_query = {'$and': [{join_field: {'$in': list(need_new)}}, query]}
        projection = {id_field: 1, join_field: 1}
        docs = await matchengine.async_db_ro[collection].find(new_query, projection).to_list(None)

        # In the old code, we used sets for this, which resulted in the id_cache requiring an absurd
        # amount of memory, since even empty sets have a large footprint. With tuples, this is
        # greatly reduced.
        for cid in need_new:
            id_cache[cid] = ()
        for doc in docs:
            key, new_val = doc[join_field], doc[id_field]
            old_val = id_cache[key]
            if new_val not in old_val:
                id_cache[key] = old_val + (new_val, )

        group_future.set_result(None)

    await asyncio.gather(*to_wait)

    return id_cache

def get_filter_out(matchengine, query_node, clinical_ids):
    if matchengine.clinical_filter.should_filter(query_node):
        return {
            cid
            for cid in clinical_ids
            if not matchengine.clinical_filter.apply_filter(query_node, matchengine._clinical_data[cid])
        }
    else:
        return set()

async def execute_queries(
    matchengine: MatchEngine,
    multi_collection_query: MultiCollectionQuery,
    clinical_ids: Set[ClinicalID],
) -> Tuple[Set[ObjectId], Dict[ClinicalID, List[MatchReason]]]:
    qncs = sorted(
        multi_collection_query.query_node_containers,
        key=lambda qnc: _get_query_order(matchengine, qnc),
    )
    # This function will execute to filter patients on extended clinical/genomic attributes
    qnc_tracker = []
    for query_node_container in qncs:
        exclusion = query_node_container.exclusion
        query_nodes = query_node_container.query_nodes
        if exclusion:
            for query_node in query_nodes:
                filter_out = get_filter_out(matchengine, query_node, clinical_ids)
                to_query = (clinical_ids - filter_out) if filter_out else clinical_ids
                id_cache = await run_query_node(matchengine, query_node, to_query)
                excluded = {cid for cid in to_query if id_cache[cid]}
                clinical_ids.difference_update(excluded)
            qnc_tracker.append((query_node_container, None))
        elif len(query_nodes) == 1:
            # Optimization for when we only have one query node
            query_node = query_nodes[0]
            filter_out = get_filter_out(matchengine, query_node, clinical_ids)
            to_query = (clinical_ids - filter_out) if filter_out else clinical_ids
            id_cache = await run_query_node(matchengine, query_node, to_query)
            included = {cid for cid in to_query if id_cache[cid]}
            clinical_ids = included
            qnc_tracker.append((query_node_container, [(query_node, id_cache, None)]))
        else:
            qn_tracker = []
            new_clinical_ids = set()
            for query_node in query_nodes:
                filter_out = get_filter_out(matchengine, query_node, clinical_ids)
                to_query = (clinical_ids - filter_out) if filter_out else clinical_ids
                id_cache = await run_query_node(matchengine, query_node, to_query)
                included = {cid for cid in to_query if id_cache[cid]}
                new_clinical_ids.update(included)
                qn_tracker.append((query_node, id_cache, included))
            clinical_ids = new_clinical_ids
            qnc_tracker.append((query_node_container, qn_tracker))

    needed_docs = defaultdict(set)
    needed_docs[('clinical', '_id')].update(clinical_ids)
    for query_node_container, qn_tracker in qnc_tracker:
        if query_node_container.exclusion:
            continue
        for query_node, id_cache, found_clinical_ids in qn_tracker:
            col, idf = query_node.raw_collection, query_node.raw_id_field
            subcache = needed_docs[(col, idf)]
            if found_clinical_ids is None:
                for cid in clinical_ids:
                    subcache.update(id_cache[cid])
            else:
                for cid in clinical_ids:
                    if cid in found_clinical_ids:
                        subcache.update(id_cache[cid])

    doc_cache = await get_docs_results(matchengine, needed_docs)

    reasons = {}
    for cid in clinical_ids:
        reason_list = []
        for query_node_container, qn_tracker in qnc_tracker:
            if query_node_container.exclusion:
                reason_list.append(MatchReason(query_node_container, True, None))
            else:
                reference_tuples = set()
                for query_node, id_cache, found_clinical_ids in qn_tracker:
                    if (found_clinical_ids is not None) and (cid not in found_clinical_ids):
                        continue
                    col, idf, ids = query_node.raw_collection, query_node.raw_id_field, id_cache[cid]
                    for rid in ids:
                        reference_tuples.add((col, idf, rid))
                reference_docs = [doc_cache[tup] for tup in sorted(reference_tuples)]
                reason_list.append(MatchReason(query_node_container, False, reference_docs))
        reasons[cid] = reason_list

    return clinical_ids, reasons


async def get_docs_results(matchengine: MatchEngine, needed_docs):
    """
    Matching criteria for clinical and extended_attributes values can be set/extended in config.json
    :param matchengine:
    :param needed_clinical:
    :param needed_extended:
    :return:
    """

    tasks = set()
    doc_cache = matchengine.cache.doc_results
    for (collection, id_field), ids in needed_docs.items():
        ids = {oid for oid in ids if (collection, id_field, oid) not in doc_cache}
        log.debug(f"Querying {collection} for {len(ids)} documents")
        projection = matchengine.match_criteria_transform.projections[collection]
        new_task = asyncio.create_task(get_docs_task(matchengine, collection, projection, id_field, sorted(ids)))
        tasks.add(new_task)

    try:
        for t in tasks:
            await t
    except BaseException as e:
        for t in tasks:
            if not t.done():
                log.warn("Cancelling docs task due to exception")
                try:
                    t.cancel()
                    await t
                except asyncio.CancelledError:
                    pass
        raise e

    log.debug("Queries completed")

    return doc_cache


async def get_docs_task(
    matchengine: MatchEngine,
    collection: str,
    projection: dict,
    id_field: str,
    ids: List[ObjectId],
):
    query = MongoQuery({id_field: {"$in": ids}})
    docs = await (matchengine.async_db_ro[collection].find(query, projection).to_list(None))
    for doc in docs:
        matchengine.cache.doc_results[(collection, id_field, doc[id_field])] = doc
