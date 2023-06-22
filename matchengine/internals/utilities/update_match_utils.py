from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

from pymongo import ReplaceOne, UpdateOne

from matchengine.internals.typing.matchengine_types import RunLogUpdateTask, UpdateTask, MongoQuery, UpdateResult
from matchengine.internals.utilities.list_utils import chunk_list

log = logging.getLogger('matchengine')
if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine

async def async_update_matches_by_protocol_no(matchengine: MatchEngine, protocol_no: str):
    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in
    the db. Delete matches by adding {is_disabled: true} and insert all new matches.
    """

    log.info(f"Updating matches for {protocol_no}")

    # Get the matches, adding updated times
    matches_by_sample_id = matchengine.matches[protocol_no]
    updated_time = datetime.datetime.now(datetime.timezone.utc)
    for matches in matches_by_sample_id.values():
        for match in matches:
            match['_updated'] = updated_time

    # Find all the sample IDs we queried
    all_sample_ids_mined = {
        matchengine.clinical_mapping[cid]
        for cid in matchengine.clinical_run_log_entries[protocol_no]
    }
    for k in matches_by_sample_id.keys():
        if k not in all_sample_ids_mined:
            raise ValueError("Generated matches for sample IDs we did not query")

    # Set up tracking of results
    to_insert, to_disable, to_enable = 0, 0, 0
    matchengine.update_trackers_by_protocol[protocol_no] = UpdateResult()

    # Collection to query
    coll = matchengine.async_db_ro[matchengine.trial_match_collection]

    # For each chunk of sample IDs:
    for sample_id_chunk in chunk_list(sorted(all_sample_ids_mined), matchengine.chunk_size):
        # List of write ops we need to perform
        ops = []

        # Get all found matches
        found_matches = [
            match
            for sample_id in sample_id_chunk
            for match in matches_by_sample_id.get(sample_id, [])
        ]

        # Get their hashes
        hashes_to_insert = set()
        for match in found_matches:
            h = match['hash']
            if h in hashes_to_insert:
                raise ValueError("Matching generated multiple matches with same hash")
            hashes_to_insert.add(h)


        # Get all existing matches with either the same hashes (to reenable or leave in place)
        # or that are not disabled (to disable them if necessary)
        current_matches_query = MongoQuery({
            matchengine.match_criteria_transform.match_trial_link_id: protocol_no,
            'sample_id': {'$in': sample_id_chunk},
            '$or': [
                {'hash': { '$in': list(hashes_to_insert) }},
                {'is_disabled': False}
            ]
        })
        projection = { "hash": 1, "is_disabled": 1, "_id": 1 }

        # For each of those found matches:
        async for match in coll.find(current_matches_query, projection):
            if (match['hash'] in hashes_to_insert) and not match['is_disabled']:
                # Leave the existing match in place:
                hashes_to_insert.remove(match['hash'])
            elif (match['hash'] in hashes_to_insert) and match['is_disabled']:
                # Reenable the match
                hashes_to_insert.remove(match['hash'])
                to_enable += 1
                ops.append(UpdateOne(
                    filter={'_id': match['_id']},
                    update={'$set': {'is_disabled': False,
                                    '_updated': updated_time}}
                ))
            else:
                # Disable the match
                to_disable += 1
                ops.append(UpdateOne(
                    filter={'_id': match['_id']},
                    update={'$set': {'is_disabled': True,
                                    '_updated': updated_time}}
                ))

        # Insert any genuinely new matches:
        for match in found_matches:
            if match['hash'] in hashes_to_insert:
                hashes_to_insert.remove(match['hash'])
                to_insert += 1
                ops.append(ReplaceOne(
                    filter={'hash': match['hash']},
                    replacement=match,
                    upsert=True
                ))

        # Enquue corresponding update tasks
        for op_chunk in chunk_list(ops, matchengine.chunk_size):
            matchengine.task_q.put_nowait(UpdateTask(op_chunk, protocol_no))

    # Wait for tasks to complete
    log.info(f"Planned changes: insert {to_insert}, disable {to_disable}, enable {to_enable}")
    await matchengine.task_q.join()
    tracker = matchengine.update_trackers_by_protocol.pop(protocol_no)
    log.info(f"Completed updates for {protocol_no}: {tracker.fmt()}")

    # Insert run log so we have a cache:
    if not matchengine.skip_run_log_entry:
        matchengine.task_q.put_nowait(RunLogUpdateTask(protocol_no))
    await matchengine.task_q.join()
