from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

from pymongo import ReplaceOne, UpdateOne
from collections import Counter

from matchengine.internals.typing.matchengine_types import RunLogUpdateTask, UpdateTask, MongoQuery, UpdateResult
from matchengine.internals.utilities.list_utils import chunk_list

log = logging.getLogger('matchengine')
if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine


async def async_update_matches_by_protocol_no(matchengine: MatchEngine, protocol_no: str, dry_run=False):
    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in
    the db. Delete matches by adding {is_disabled: true} and insert all new matches.
    """

    log.info(f"Updating matches for {protocol_no}")

    # Get the matches, adding updated times
    matches_by_sample_id = matchengine.matches[protocol_no]
    updated_time = datetime.datetime.now(datetime.timezone.utc)

    # Find all the sample IDs we queried
    all_sample_ids_mined = {
        matchengine.clinical_mapping[cid]
        for cid in matchengine.get_clinical_ids_for_protocol(protocol_no)
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
        counts_by_hash = Counter( m['hash'] for m in found_matches )

        # Get all existing matches with either the same hashes (to reenable or leave in place)
        # or that are not disabled (to disable them if necessary)
        current_matches_query = MongoQuery({
            matchengine.match_criteria_transform.match_trial_link_id: protocol_no,
            'sample_id': {'$in': sample_id_chunk},
            '$or': [
                {'hash': { '$in': list(counts_by_hash) }},
                {'is_disabled': False}
            ]
        })
        projection = { "hash": 1, "is_disabled": 1, "_id": 1 }
        query_matches = await coll.find(current_matches_query, projection).to_list(None)
        query_matches = sorted(query_matches, key=lambda m: m['_id'])

        # First, preserve as many existing matches as possible:
        potentially_modify = []
        for match_info in query_matches:
            h = match_info['hash']
            is_disabled = match_info['is_disabled']
            if counts_by_hash[h] and not is_disabled:
                counts_by_hash.subtract([h])
            else:
                potentially_modify.append(match_info)

        # Next, reenable as many existing matches as possible:
        potentially_disable = []
        for match_info in potentially_modify:
            h = match_info['hash']
            is_disabled = match_info['is_disabled']
            if counts_by_hash[h]:
                counts_by_hash.subtract([h])
                if is_disabled:
                    to_enable += 1
                    ops.append(UpdateOne(
                        filter={'_id': match_info['_id']},
                        update={'$set': {'is_disabled': False,
                                        '_updated': updated_time}}
                    ))
            else:
                potentially_disable.append(match_info)

        # Next, disable all others:
        for match_info in potentially_disable:
            h = match_info['hash']
            is_disabled = match_info['is_disabled']
            if not is_disabled:
                to_disable += 1
                ops.append(UpdateOne(
                    filter={'_id': match_info['_id']},
                    update={'$set': {'is_disabled': True,
                        '_updated': updated_time}}
                ))

        # Finally, insert new matches:
        for match_doc in found_matches:
            h = match_doc['hash']
            if counts_by_hash[h]:
                counts_by_hash.subtract([h])
                to_insert += 1
                match_doc['_updated'] = updated_time
                match_doc['_me_sequence_number'] = matchengine.get_sequence_number()
                match_doc['_me_id'] = matchengine.run_id.hex
                # The "sequence number" guarantees idempotency
                ops.append(ReplaceOne(
                    filter={
                        k: v for k, v in match_doc.items()
                        if k in ('hash', '_me_id', '_me_sequence_number')
                    },
                    replacement=match_doc,
                    upsert=True
                ))

        if ops and dry_run:
            log.warn("Dry run, ignoring changes:")
            for op in ops:
                log.warn(f"CHANGE: {op!r}")
            ops = []

        if not dry_run:
            # Enqueue corresponding update tasks
            for op_chunk in chunk_list(ops, matchengine.chunk_size):
                matchengine.task_q.put_nowait(UpdateTask(op_chunk, protocol_no))

    # Wait for tasks to complete
    log.info(f"Planned changes: insert {to_insert}, disable {to_disable}, enable {to_enable}")
    await matchengine.task_q.join()
    tracker = matchengine.update_trackers_by_protocol.pop(protocol_no)
    if not dry_run:
        log.info(f"Completed updates for {protocol_no}: {tracker.fmt()}")

    # Insert run log so we have a cache:
    if not matchengine.skip_run_log_entry and not dry_run:
        matchengine.task_q.put_nowait(RunLogUpdateTask(protocol_no))
    await matchengine.task_q.join()
