from __future__ import annotations

import asyncio
import logging
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING, List, Dict

from pymongo import InsertOne
from pymongo.errors import (
    AutoReconnect,
    CursorNotFound,
    ServerSelectionTimeoutError)

from matchengine.internals.utilities.list_utils import chunk_list
from matchengine.internals.typing.matchengine_types import (
    TrialMatch, IndexUpdateTask,
    MatchReason, UpdateTask,
    RunLogUpdateTask, ClinicalID
)
from matchengine.internals.utilities.object_comparison import nested_object_hash

if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine

log = logging.getLogger('matchengine')


async def run_check_indices_task(matchengine: MatchEngine, task, worker_id):
    """
    Ensure indexes exist on collections so queries are performant
    """
    if matchengine.debug:
        log.info(
            f"Worker: {worker_id}, got new CheckIndicesTask")
    try:
        for collection, desired_indices in matchengine.config['indices'].items():
            if collection == "trial_match":
                collection = matchengine.trial_match_collection

            index_info = matchengine.db_ro[collection].index_information()
            existing = set()
            for name, info in index_info.items():
                key = tuple( field for field, direction in info['key'] )
                existing.add(key)

            desired = set()
            for index in desired_indices:
                if not isinstance(index, list):
                    index = [index]
                desired.add(tuple(index))

            for fields in (desired - existing):
                matchengine.task_q.put_nowait(IndexUpdateTask(
                    collection,
                    [ (f, 1) for f in fields ]
                ))
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            await matchengine.task_q.put(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.__exit__(None, None, None)
            matchengine.loop.stop()
            log.error((f"ERROR: Worker: {worker_id}, error: {e}"
                       f"TRACEBACK: {traceback.print_tb(e.__traceback__)}"))
            raise e


async def run_index_update_task(matchengine: MatchEngine, task: IndexUpdateTask, worker_id):
    if matchengine.debug:
        log.info(
            f"Worker: {worker_id}, index {task.index}, collection {task.collection} got new IndexUpdateTask")
    try:
        log.info(f"Creating index: {task.index!r}")
        matchengine.db_rw[task.collection].create_index(task.index)
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.loop.stop()
            log.error((f"ERROR: Worker: {worker_id}, error: {e}"
                       f"TRACEBACK: {traceback.print_tb(e.__traceback__)}"))


async def run_query_task(matchengine: MatchEngine, task, worker_id):
    trial_identifier = matchengine.match_criteria_transform.trial_identifier
    protocol_no = task.trial[trial_identifier]
    if matchengine.debug:
        log.info((f"Worker: {worker_id}, {trial_identifier}: {protocol_no} got new QueryTask, "
                    f"{matchengine._task_q.qsize()} tasks left in queue"))
    clinical_ids = matchengine.get_clinical_ids_for_protocol(protocol_no)
    if not clinical_ids:
        log.info("No clinical IDs for query task, skipping")
        matchengine.task_q.task_done()
        return
    try:
        results, docs = await matchengine.run_query(task.query, clinical_ids)
    except Exception as e:
        results = dict()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.loop.stop()
            log.error(f"ERROR: Worker: {worker_id}, error: {e}")
            log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
            raise e

    try:
        matchengine.cache.docs = docs
        trial_match_dict = matchengine.matches[task.trial[trial_identifier]]
        for clinical_id, sample_reasons in results.items():
            trial_match = TrialMatch(task.trial,
                                            task.match_clause_data,
                                            task.match_path,
                                            task.query,
                                            sample_reasons,
                                            clinical_id)

            # generate trial match documents using plugin
            match_documents = matchengine.create_trial_matches(trial_match)

            sample_id = matchengine.clinical_mapping[clinical_id]
            sample_match_list = trial_match_dict.setdefault(sample_id, [])

            for match_document in match_documents:
                match_document[matchengine.match_criteria_transform.match_trial_link_id] = protocol_no
                match_document['sample_id'] = sample_id
                # generate sort_order and hash fields after all fields are added
                match_hash = nested_object_hash(match_document)
                match_document['hash'] = match_hash
                match_document['is_disabled'] = False

                sample_match_list.append(match_document)
    except Exception as e:
        matchengine.loop.stop()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        raise e

    matchengine.task_q.task_done()


async def run_poison_pill(matchengine: MatchEngine, task, worker_id):
    if matchengine.debug:
        log.info(f"Worker: {worker_id} got PoisonPill")
    matchengine.task_q.task_done()


async def run_update_task(matchengine: MatchEngine, task: UpdateTask, worker_id):
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new UpdateTask {task.protocol_no}")
        tasks = [
            matchengine.async_db_rw[matchengine.trial_match_collection].bulk_write(chunked_ops,
                                                                                   ordered=False)
            for chunked_ops
            in chunk_list(task.ops, matchengine.chunk_size)
        ]
        results = await asyncio.gather(*tasks)
        local_update_tracker = matchengine.update_trackers_by_protocol[task.protocol_no]
        global_update_tracker = matchengine.global_update_tracker
        for result in results:
            local_update_tracker.add_bulk_write_result(result)
            if global_update_tracker:
                global_update_tracker.add_bulk_write_result(result)
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.task_done()
            matchengine.task_q.put_nowait(task)
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            raise e


async def run_run_log_update_task(matchengine: MatchEngine, task: RunLogUpdateTask, worker_id):
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new RunLogUpdateTask {task.protocol_no}")

        await matchengine.record_run_log(task.protocol_no)

        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.task_done()
            matchengine.task_q.put_nowait(task)
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            raise e
