from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import csv
import uuid
from collections import defaultdict
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Iterable, Tuple

import dateutil.parser
import pymongo
from pymongo import UpdateMany

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.match_criteria_transform import MatchCriteriaTransform
from matchengine.internals.match_translator import (
    extract_match_clauses_from_trial,
    create_match_tree,
    get_match_paths,
    translate_match_path
)
from matchengine.internals.typing.matchengine_types import (
    PoisonPill,
    Cache,
    QueryTask,
    UpdateTask,
    RunLogUpdateTask,
    CheckIndicesTask,
    IndexUpdateTask,
    UpdateResult
)
from matchengine.internals.utilities.query import (
    execute_clinical_queries,
    execute_extended_queries,
    get_docs_results
)
from matchengine.internals.utilities.task_utils import (
    run_query_task,
    run_poison_pill,
    run_update_task,
    run_run_log_update_task,
    run_check_indices_task, run_index_update_task
)
from matchengine.internals.utilities.update_match_utils import async_update_matches_by_protocol_no
from matchengine.internals.utilities.utilities import (
    find_plugins
)

if TYPE_CHECKING:
    from typing import (
        NoReturn,
        Any
    )
    from matchengine.internals.typing.matchengine_types import (
        Dict,
        Union,
        List,
        Set,
        ClinicalID,
        MultiCollectionQuery,
        MatchReason,
        ObjectId,
        Trial,
        QueryNode,
        TrialMatch,
        Task,
        QueryNodeContainer
    )

log = logging.getLogger('matchengine')


class MatchEngine(object):
    cache: Cache
    config: Dict
    match_criteria_transform: MatchCriteriaTransform
    protocol_nos: Union[List[str], None]
    sample_ids: Union[List[str], None]
    match_on_closed: bool
    match_on_deceased: bool
    debug: bool
    num_workers: int
    clinical_ids: Set[ClinicalID]
    _task_q: asyncio.queues.Queue
    _matches: Dict[str, Dict[str, List[Dict]]]
    _loop: asyncio.AbstractEventLoop
    _workers: Dict[int, asyncio.Task]
    update_trackers_by_protocol: Dict[str, UpdateResult]
    global_update_tracker: Union[UpdateResult, None]

    def __enter__(self):
        return self

    async def _async_exit(self):
        """
        Ensure that all async workers exit gracefully.
        """
        for _ in range(0, self.num_workers):
            self._task_q.put_nowait(PoisonPill())
        await self._task_q.join()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Teardown database connections (async + synchronous) and async workers gracefully.
        """
        self._async_db_ro.__exit__(exception_type, exception_value, exception_traceback)
        self._async_db_rw.__exit__(exception_type, exception_value, exception_traceback)
        self._db_ro.__exit__(exception_type, exception_value, exception_traceback)
        self._db_rw.__exit__(exception_type, exception_value, exception_traceback)
        if not self.loop.is_closed():
            self._loop.run_until_complete(self._async_exit())
            self._loop.close()

    def __init__(
            self,
            sample_ids: Set[str] = None,
            protocol_nos: Set[str] = None,
            match_on_deceased: bool = False,
            match_on_closed: bool = False,
            debug: bool = False,
            num_workers: int = cpu_count() * 5,
            visualize_match_paths: bool = False,
            fig_dir: str = None,
            config: Union[str, dict] = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'config',
                'dfci_config.json'),
            plugin_dir: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plugins'),
            db_name: str = None,
            match_document_creator_class: str = "DFCITrialMatchDocumentCreator",
            query_node_transformer_class: str = "DFCIQueryNodeTransformer",
            query_node_subsetter_class: str = "DFCIQueryNodeClinicalIDSubsetter",
            query_node_container_transformer_class: str = "DFCIQueryContainerTransformer",
            db_secrets_class: str = None,
            ignore_run_log: bool = False,
            skip_run_log_entry: bool = False,
            trial_match_collection: str = "trial_match",
            drop: bool = False,
            exit_after_drop: bool = False,
            drop_accept: bool = False,
            resource_dirs: List = None,
            chunk_size: int = 1000,
            age_comparison_date = None,
            delete_run_logs = False,
            start_time_utc = None
    ):
        self.resource_dirs = list()
        self.update_trackers_by_protocol = {}
        self.global_update_tracker = None
        self.resource_dirs.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ref'))
        if resource_dirs is not None:
            self.resource_dirs.extend(resource_dirs)
        self.trial_match_collection = trial_match_collection
        self.age_comparison_date = age_comparison_date or datetime.date.today()
        self.start_time_utc = start_time_utc or datetime.datetime.now(datetime.timezone.utc)
        self.run_id = uuid.uuid4()
        log.info(f"initializing matchengine with run id: {self.run_id.hex}")
        log.info(f"age comparison date: {age_comparison_date}")
        self.run_log_entries = dict()
        self.ignore_run_log = ignore_run_log
        self.skip_run_log_entry = skip_run_log_entry
        self._protocol_nos_param = list(protocol_nos) if protocol_nos is not None else protocol_nos
        self._sample_ids_param = list(sample_ids) if sample_ids is not None else sample_ids
        self.chunk_size = chunk_size
        self.debug = debug

        if config.__class__ is str:
            with open(config) as config_file_handle:
                self.config = json.load(config_file_handle)
        else:
            self.config = config

        self.match_criteria_transform = MatchCriteriaTransform(self.config, self.resource_dirs)

        self.plugin_dir = plugin_dir
        self.match_document_creator_class = match_document_creator_class
        self.query_node_transformer_class = query_node_transformer_class
        self.query_node_container_transformer_class = query_node_container_transformer_class
        self.query_node_subsetter_class = query_node_subsetter_class
        self.db_secrets_class = db_secrets_class
        find_plugins(self)

        self._db_ro = MongoDBConnection(read_only=True, async_init=False,
                                        db=db_name)
        self.db_ro = self._db_ro.__enter__()
        self._db_rw = MongoDBConnection(read_only=False, async_init=False,
                                        db=db_name)
        self.db_rw = self._db_rw.__enter__()
        log.info(f"connected to database {self.db_ro.name}")

        if delete_run_logs:
            log.info("deleting run logs")
            run_log_collection = 'run_log_' + trial_match_collection
            res = self.db_rw.get_collection(run_log_collection).delete_many({})
            log.info(f"deleted {res.deleted_count} items")

        self._drop = drop
        if self._drop:
            log.info((f"Dropping all matches"
                      "\n\t"
                      f"{f'for trials: {protocol_nos}' if protocol_nos is not None else 'all trials'}"
                      "\n\t"
                      f"{f'for samples: {sample_ids}' if sample_ids is not None else 'all samples'}"
                      "\n"
                      f"{'and then exiting' if exit_after_drop else 'and then continuing'}"))
            try:
                assert drop_accept or input(
                    'Type "yes" without quotes in all caps to confirm: ') == "YES"
                self.drop_existing_matches(protocol_nos, sample_ids)
            except AssertionError:
                log.error("Your response was not 'YES'; exiting")
                exit(1)
            if exit_after_drop:
                exit(0)

        # A cache-like object used to accumulate query results
        self.cache = Cache()
        self.match_on_closed = match_on_closed
        self.match_on_deceased = match_on_deceased
        self.num_workers = num_workers
        self.visualize_match_paths = visualize_match_paths
        self.fig_dir = fig_dir
        self._matches = {}
        self._clinical_ids_by_protocol = {}

        self.trials = self.get_trials(protocol_nos)
        if protocol_nos is None:
            self.protocol_nos = sorted(self.trials.keys())
        else:
            self.protocol_nos = protocol_nos
        self._run_log_history = self._populate_run_log_history()

        self._clinical_data = self._get_clinical_data(sample_ids)
        self.clinical_mapping = self.get_clinical_ids_from_sample_ids()
        self.clinical_deceased = self.get_clinical_deceased()
        self.clinical_birth_dates = self.get_clinical_birth_dates()
        self.clinical_update_mapping = self.get_clinical_updated_mapping()
        self.clinical_extra_field_lookup = self.get_extra_field_lookup(self._clinical_data,
                                                                       "clinical")
        self.clinical_ids = set(self.clinical_mapping.keys())

        # instantiate a new async event loop to allow class to be used as if it is synchronous
        # NOTE: asyncio's "run_until_complete" function suffices to ensure that we're
        # not nesting event loops.
        self._loop = asyncio.new_event_loop()
        self._loop.slow_callback_duration = 0.5 # Reduce logging noise

        # create async mongo connections for event loop
        self._async_db_ro = MongoDBConnection(read_only=True, db=db_name, loop=self._loop)
        self.async_db_ro = self._async_db_ro.__enter__()
        self._async_db_rw = MongoDBConnection(read_only=False, db=db_name, loop=self._loop)
        self.async_db_rw = self._async_db_rw.__enter__()

        # create a task queue for async tasks
        self._task_q = asyncio.queues.Queue(loop=self._loop)

        self._sequence_number = 0

        # create "workers" which handle async tasks from the task_q
        # general pattern is to put a series of tasks in the queue, then await task_q.join()
        # and the workers will complete the tasks.
        # this is done instead of using asyncio.gather as the event loop will hang if >100s of coroutines/futures
        # are placed at once, especially if they're I/O related. The epoll (linux) and kqueue (macOS/BSD)
        # selectors used in the event loop implementation will grind to a halt with too many open sockets, and
        # as we can have 1000's of requests for a single trial, we need to limit the effective I/O concurrency.
        # In effect, the effective concurrency is the number of workers.
        self._workers = {
            worker_id: self._loop.create_task(self._queue_worker(worker_id))
            for worker_id in range(0, self.num_workers)
        }

        self._loop.run_until_complete(self._async_init())

    def create_output_csv(self):
        """Generate output CSV file from all generated trial_match documents"""

        fieldnames = set()
        for protocol_no in self.matches:
            for sample_id in self.matches[protocol_no]:
                for match in self.matches[protocol_no][sample_id]:
                    fieldnames.update(match.keys())

        # write CSV
        with open(f'trial_matches_{datetime.datetime.now().strftime("%b_%d_%Y_%H:%M")}.csv', 'a') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=fieldnames)
            writer.writeheader()
            for protocol_no, samples in self.matches.items():
                for sample_id, matches in samples.items():
                    for match in matches:
                        writer.writerow(match)

    def get_sequence_number(self):
        # Gets a number unique to the current MatchEngine instance.
        # Used as an idempotency key when updating matches.
        x = self._sequence_number
        self._sequence_number += 1
        return x

    async def _async_init(self):
        """
        Instantiate asynchronous db connections and workers.
        Create a task que which holds all matching and update tasks for processing via workers.
        """
        self._task_q.put_nowait(CheckIndicesTask())
        await self._task_q.join()

    async def run_query(self,
                        multi_collection_query: MultiCollectionQuery,
                        clinical_ids: Set[ClinicalID]) -> Dict[MatchReason]:
        """
        Execute a mongo query on the clinical and extended_attributes collections to find trial matches.
        First execute the clinical query. If no records are returned short-circuit and return.
        """
        # Note: preserve copy here so later things don't mutate it
        new_clinical_ids = clinical_ids - (set() if self.match_on_deceased else self.clinical_deceased)
        log.debug(f"Vital status filter narrowed {len(clinical_ids)} samples to {len(new_clinical_ids)}")
        clinical_ids = new_clinical_ids

        new_clinical_ids, clinical_match_reasons = \
            await execute_clinical_queries(self, multi_collection_query, set(clinical_ids))
        log.debug(f"Clinical queries narrowed {len(clinical_ids)} samples to {len(new_clinical_ids)}")
        clinical_ids = new_clinical_ids

        new_clinical_ids, _, all_match_reasons = await (
            execute_extended_queries(self,
                                        multi_collection_query,
                                        set(clinical_ids),
                                        clinical_match_reasons)
        )
        log.debug(f"Extended queries narrowed {len(clinical_ids)} samples to {len(new_clinical_ids)}")
        clinical_ids = new_clinical_ids

        old_len = len(all_match_reasons)
        for k in list(all_match_reasons.keys()):
            if k not in clinical_ids:
                del all_match_reasons[k]
            if len(all_match_reasons[k]) == 0:
                del all_match_reasons[k]
        log.debug(f"Postprocessing narrowed {old_len} samples to {len(all_match_reasons)}")

        docs = await get_docs_results(self, all_match_reasons)

        return all_match_reasons, docs

    async def _queue_worker(self, worker_id: int) -> None:
        """
        Function which executes tasks placed on the task queue.
        """
        while True:
            # Execute update task
            task: Task = await self._task_q.get()
            args = (self, task, worker_id)
            task_class = task.__class__
            if task_class is PoisonPill:
                await run_poison_pill(*args)
                break

            elif task_class is QueryTask:
                await run_query_task(*args)

            elif task_class is UpdateTask:
                await run_update_task(*args)

            elif task_class is RunLogUpdateTask:
                await run_run_log_update_task(*args)

            elif task_class is CheckIndicesTask:
                await run_check_indices_task(*args)

            elif task_class is IndexUpdateTask:
                await run_index_update_task(*args)

    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
        """Stub function to be overriden by plugin"""
        pass

    def query_node_container_transform(self, query_node_container: QueryNodeContainer) -> NoReturn:
        """Stub function to be overriden by plugin"""
        pass

    def extended_query_node_clinical_ids_subsetter(
            self,
            query_node: QueryNode,
            clinical_ids: Iterable[ClinicalID]
    ) -> Tuple[bool, Set[ClinicalID]]:
        """Stub function to be overriden by plugin"""
        return {clinical_id for clinical_id in clinical_ids}

    def update_matches_for_protocol_number(self, protocol_no: str, dry_run = False):
        """
        Updates all trial matches for a given protocol number
        """
        self._loop.run_until_complete(async_update_matches_by_protocol_no(self, protocol_no, dry_run))

    def update_all_matches(self, dry_run=False):
        """
        Synchronously iterates over each protocol number, updating the matches in the database for each
        """
        log.info("Updating all matches")
        self.global_update_tracker = UpdateResult()

        if self._protocol_nos_param is None and not self._drop and not dry_run:
            log.info("Updating matches for deleted protocols")
            self.update_trackers_by_protocol['DELETED_PROTOCOLS'] = UpdateResult()
            updated_time = datetime.datetime.now(datetime.timezone.utc)
            match_identifier = self.match_criteria_transform.match_trial_link_id
            self.task_q.put_nowait(
                UpdateTask(
                    [UpdateMany({match_identifier: {'$nin': self.protocol_nos}, 'is_disabled': False},
                                {'$set': {'is_disabled': True, '_updated': updated_time}})],
                    'DELETED_PROTOCOLS'))
            task = self._loop.create_task(self._task_q.join())
            self._loop.run_until_complete(task)
            upd = self.update_trackers_by_protocol.pop('DELETED_PROTOCOLS')
            log.info(f"Updated matches for deleted protocols: {upd.fmt()}")

        for protocol_number in self.protocol_nos:
            self.update_matches_for_protocol_number(protocol_number, dry_run)

        if not dry_run:
            log.info(f"Updated all matches: {self.global_update_tracker.fmt()}")
        self.global_update_tracker = None

    def get_matches_for_all_trials(self) -> Dict[str, Dict[str, List]]:
        """
        Synchronously iterates over each protocol number, getting trial matches for each
        """

        task = self._loop.create_task(self._async_get_matches_for_all_trials())
        return self._loop.run_until_complete(task)

    def get_matches_for_trial(self, protocol_no):
        task = self._loop.create_task(self._async_get_matches_for_trial(protocol_no))
        return self._loop.run_until_complete(task)

    async def _async_get_matches_for_all_trials(self):
        """
        Get the trial matches for a given protocol number
        """
        for protocol_no in self.protocol_nos:
            await self._async_get_matches_for_trial(protocol_no)


    async def _async_get_matches_for_trial(self, protocol_no):
        """
        Asynchronous function used by get_matches_for_trial, not meant to be called externally.
        Gets the matches for a given trial
        """
        tasks, clinical_ids_to_run = self._get_tasks_and_clinical_ids_to_run(protocol_no)
        self.create_run_log_entry(protocol_no)
        self._clinical_ids_by_protocol[protocol_no] = clinical_ids_to_run
        self._matches[protocol_no] = dict()
        log.info(f"Mining matches for {protocol_no}: checking {len(clinical_ids_to_run)} of {len(self.clinical_ids)} samples")
        if not tasks:
            log.debug("No queries to run")
        else:
            for task in tasks:
                self._task_q.put_nowait(task)
            await self._task_q.join()
        num_found = sum( 1 for matches in self._matches.get(protocol_no, {}).values() if len(matches) )
        num_docs = sum( len(matches) for matches in self._matches.get(protocol_no, {}).values() )
        log.info(f"Mined matches for {protocol_no}: {num_docs} matches found across {num_found} samples")

    def _get_tasks_and_clinical_ids_to_run(self, protocol_no: str) -> Dict[str, List[Dict]]:
        tasks = list()
        age_criteria = set()
        trial = self.trials[protocol_no]
        if self.match_on_closed or self._trial_is_open(trial):
            # Get each match clause in the trial document
            match_clauses = extract_match_clauses_from_trial(self, trial)

            # for each match clause, create the match tree, and extract each possible match path from the tree

            for match_clause in match_clauses:
                if match_clause.is_suspended and not self.match_on_closed:
                    continue
                match_tree = create_match_tree(self, match_clause)
                match_paths = get_match_paths(match_tree)

                # for each match path, translate the path into valid mongo queries
                for match_path in match_paths:
                    query = translate_match_path(self, match_clause, match_path)
                    for criteria_node in match_path.criteria_list:
                        for criteria in criteria_node.criteria:
                            # check if node has any age criteria, to know to check for newly qualifying patients
                            # or patients aging out
                            for k, v in criteria.get('clinical', dict()).items():
                                if k.lower() == 'age_numerical':
                                    age_criteria.add(v)
                    # put the query onto the task queue for execution
                    tasks.append(QueryTask(trial, match_clause, match_path, query))
            ignore_clinical_updates = False
        else:
            log.info('Trial is closed and match_on_closed is false')
            ignore_clinical_updates = True

        clinical_ids_to_run = self._get_clinical_ids_to_run(
            self.clinical_ids,
            age_criteria,
            self._run_log_history[protocol_no],
            ignore_clinical_updates
        )
        return tasks, clinical_ids_to_run


    async def record_run_log(self, protocol_no):
        run_log_collection = f"run_log_{self.trial_match_collection}"
        await self.async_db_rw[run_log_collection].insert_one(self.run_log_entries[protocol_no])

    def _populate_run_log_history(self) -> Dict[str, List[Dict]]:
        """
        Get all run log entries for trial after trial's last updated date.
        :return:
        """
        default_datetime = datetime.datetime.strptime('January 01, 0001', '%B %d, %Y')
        run_log_entries_by_protocol = dict()
        for protocol_no in self.protocol_nos:
            trial = self.trials[protocol_no]
            trial_last_update = trial.get('_updated', default_datetime)
            query = {self.match_criteria_transform.match_trial_link_id: protocol_no, "start_time_utc": {'$gte': trial_last_update}}
            cursor = self.db_ro[f"run_log_{self.trial_match_collection}"].find(query).sort(
                [("start_time_utc", pymongo.DESCENDING)])

            run_log_entries_by_protocol[protocol_no] = list(cursor)
        return run_log_entries_by_protocol

    def _get_clinical_data(self, sample_ids):
        # if no sample ids are passed in as args, get all clinical documents
        query: Dict = {}
        if sample_ids is not None:
            query.update({"SAMPLE_ID": {"$in": list(sample_ids)}})
        projection = {
            '_id': 1,
            'SAMPLE_ID': 1,
            'VITAL_STATUS': 1,
            'BIRTH_DATE_INT': 1,
            '_updated': 1,
        }
        projection.update({
            item[0]: 1
            for item
            in self.config.get("extra_initial_lookup_fields", dict()).get("clinical", list())})
        return {result['_id']: result
                for result in
                self.db_ro.clinical.find(query, projection)}

    def get_clinical_updated_mapping(self) -> Dict[ObjectId: datetime.datetime]:
        return {clinical_id: clinical_data.get('_updated', None) for clinical_id, clinical_data in
                self._clinical_data.items()}

    def get_clinical_deceased(self) -> Set[ClinicalID]:
        return {clinical_id
                for clinical_id, clinical_data
                in self._clinical_data.items()
                if clinical_data['VITAL_STATUS'] == 'deceased'}

    def get_clinical_birth_dates(self) -> Dict[ClinicalID, int]:
        return {clinical_id: clinical_data['BIRTH_DATE_INT']
                for clinical_id, clinical_data
                in self._clinical_data.items()
                }

    def get_clinical_ids_from_sample_ids(self) -> Dict[ClinicalID, str]:
        """
        Create a map of clinical IDs to sample IDs
        """
        return {clinical_id: clinical_data['SAMPLE_ID'] for clinical_id, clinical_data in
                self._clinical_data.items()}

    def get_trials(self, protocol_nos) -> Dict[str, Trial]:
        """
        Gets all the trial documents in the database, or just the relevant trials (if protocol numbers supplied)
        """
        trial_find_query = dict()

        # matching criteria can be set and extended in config.json. for more details see the README
        projection = self.match_criteria_transform.projections[self.match_criteria_transform.trial_collection]
        trial_identifier = self.match_criteria_transform.trial_identifier

        if protocol_nos is not None:
            trial_find_query[trial_identifier] = {
                "$in": [protocol_no for protocol_no in protocol_nos]
            }

        all_trials = {
            result[trial_identifier]: result
            for result in
            self.db_ro[self.match_criteria_transform.trial_collection].find(trial_find_query,
                                  dict({"_updated": 1, "last_updated": 1}, **projection))
        }
        return all_trials

    def _trial_is_open(self, trial) -> bool:
        summary_status_open = trial.get("_summary", dict()).get("status", [dict()])[0].get("value", str()).lower() in {"open to accrual"}

        # By default, first check if _summary.status.value: "open to accrual"
        # as this is DFCI's default implementation
        if summary_status_open:
            return True

        # Otherwise, use trial status configuration as defined in config.json
        elif self.match_criteria_transform.use_custom_trial_status_key is not None and \
                self.match_criteria_transform.custom_status_key_name in trial:

            # be case insensitive when checking trial open/close status
            trial_status_val = trial[self.match_criteria_transform.custom_status_key_name]
            trial_status_val = trial_status_val.lower().strip() if isinstance(trial_status_val, str) else trial_status_val

            if trial_status_val in self.match_criteria_transform.custom_open_to_accrual_vals:
                return True

        return False


    def create_run_log_entry(self, protocol_no):
        """
        Create a record of a matchengine run by protocol no.
        Include clinical ids ran during run. 'all' meaning all sample ids in the db, or a subsetted list
        Include original arguments.
        """
        run_log_clinical_ids_new = dict()
        if self._sample_ids_param is None:
            run_log_clinical_ids_new['all'] = None
        else:
            run_log_clinical_ids_new['list'] = list(self.clinical_ids)

        self.run_log_entries[protocol_no] = {
            self.match_criteria_transform.match_trial_link_id: protocol_no,
            'clinical_ids': run_log_clinical_ids_new,
            'run_id': self.run_id.hex,
            'run_params': {
                self.match_criteria_transform.trial_collection: self._protocol_nos_param,
                'sample_ids': self._sample_ids_param,
                'match_on_deceased': self.match_on_deceased,
                'match_on_closed': self.match_on_closed,
                'workers': self.num_workers,
                'ignore_run_log': self.ignore_run_log
            },
            '_created': datetime.datetime.now(),
            'start_time_utc': self.start_time_utc,
            'age_comparison_date': self.age_comparison_date.isoformat()
        }

    def get_clinical_ids_for_protocol(self, protocol_no: str):
        return self._clinical_ids_by_protocol[protocol_no]

    def _get_clinical_ids_to_run(
        self,
        clinical_ids: Set[ClinicalID],
        age_criteria: Set[str],
        run_log_entries: list,
        ignore_clinical_updates: bool = False,
    ) -> Set(ObjectId):
        """
        Gets the updated/aged clinical IDs we need to match against for a given trial,
        or all clinical IDs if the protocol has been updated since the last run.
        """

        if self.ignore_run_log:
            log.debug("Ignoring run log entries")
            return clinical_ids

        if not run_log_entries:
            log.debug("No run log entries")
            return clinical_ids


        run_log = run_log_entries[0]
        if 'all' not in run_log['clinical_ids']:
            rl_clinical_ids = set(run_log['clinical_ids']['list'])
            clinical_ids, remaining_ids = clinical_ids & rl_clinical_ids, clinical_ids - rl_clinical_ids
        else:
            remaining_ids = set()

        # Note: if you change match_on_closed or match_on_deceased, or if age_comparison_date
        # is decreased between runs, it is possible that we won't entirely clean up after a previous
        # crashed run. Otherwise, we only expand the list of potentially changed records over time,
        # so this won't be an issue. Likewise, we assume that no runs will occur simultaneously
        # (i.e. no potential concurrency issues).

        moc_clinical_ids = set()
        if run_log['run_params']['match_on_closed'] != self.match_on_closed:
            # Note: must recheck all clinical IDs because match_on_closed also affects how e.g.
            # arms are interpreted
            log.debug("Match on closed differs; must retest all samples")
            moc_clinical_ids = clinical_ids

        mod_clinical_ids = set()
        if run_log['run_params']['match_on_deceased'] != self.match_on_deceased:
            log.debug("Match on deceased differs")
            mod_clinical_ids = clinical_ids & self.clinical_deceased

        # Get records updated since last run
        updated_clinical_ids = set()
        if not ignore_clinical_updates:
            prev_run_start_time = run_log['start_time_utc']
            extra_time = datetime.timedelta(seconds=60) # compensate for any clock skew
            prev_run_start_time = prev_run_start_time - extra_time
            log.debug(
                f"Checking for updates between "
                f"{prev_run_start_time.isoformat(timespec='seconds')} "
                f"and {self.start_time_utc.isoformat(timespec='seconds')}"
            )
            for clinical_id in clinical_ids:
                updated_at = self.clinical_update_mapping[clinical_id]
                if updated_at is None or updated_at > prev_run_start_time:
                    updated_clinical_ids.add(clinical_id)
            log.debug(f"Potentially modified records: {len(updated_clinical_ids)}")

        # Get records that have aged in/out of trial criteria
        # NOTE: we can safely assume that age_comparison_date never decreases between runs;
        # if that weren't the case, we could fail to detect some changes if one run of MatchEngine crashed.
        aged_clinical_ids = set()
        if not age_criteria:
            log.debug('No age-related criteria, skipping aging check')
        else:
            last_age_comparison_date = datetime.date.fromisoformat(run_log['age_comparison_date'])
            if last_age_comparison_date == self.age_comparison_date:
                log.debug(f'Same age comparison date {self.age_comparison_date}, skipping aging check')
            else:
                for age_criterion in age_criteria:
                    results = self.get_newly_qualifying_patients(
                        clinical_ids,
                        age_criterion,
                        last_age_comparison_date
                    )
                    log.debug(f"Age criterion {age_criterion!r} means {len(results)} patients may have aged in/out")
                    aged_clinical_ids.update(results)
                log.debug(f"Potentially aged records: {len(aged_clinical_ids)}")


        clinical_ids_to_run = moc_clinical_ids | mod_clinical_ids | updated_clinical_ids | aged_clinical_ids

        log.debug(f"IDs to run: {len(clinical_ids_to_run)}")

        if remaining_ids:
            log.debug("Recursing to next run log")
            recursive_results = self._get_clinical_ids_to_run(
                remaining_ids,
                age_criteria,
                run_log_entries[1:],
                ignore_clinical_updates
            )
            clinical_ids_to_run |= recursive_results

        return clinical_ids_to_run

    def get_newly_qualifying_patients(self, clinical_ids, age_criterion, last_age_comparison_date):
        """
        # This function handles all the logic for when patients age in and out of trials, for when the run log
        # would otherwise skip them
        :return:
        """
        age_range_to_date_query = getattr(self.match_criteria_transform.query_transformers,
                                          'age_range_to_date_int_query')
        result_criteria_key_map = {
            '$lte': lambda x, y: x <= y,
            '$gte': lambda x, y: x >= y,
            '$eq': lambda x, y: x == y,
            '$lt': lambda x, y: x < y,
            '$gt': lambda x, y: x > y
        }

        old_criterion = age_range_to_date_query(
            sample_key=None,
            trial_value=age_criterion,
            compare_date=last_age_comparison_date
        ).results[0].query[None]

        new_criterion = age_range_to_date_query(
            sample_key=None,
            trial_value=age_criterion,
            compare_date=self.age_comparison_date
        ).results[0].query[None]

        old_criterion_parts = list(old_criterion.items())
        assert len(old_criterion_parts) == 1
        old_criterion_op, old_criterion_val = old_criterion_parts[0]

        new_criterion_parts = list(new_criterion.items())
        assert len(new_criterion_parts) == 1
        new_criterion_op, new_criterion_val = new_criterion_parts[0]

        assert old_criterion_op == new_criterion_op
        op = old_criterion_op

        op_func = result_criteria_key_map[op]
        ids_aged = set()
        for clinical_id in clinical_ids:
            if clinical_id in self.clinical_deceased and not self.match_on_deceased:
                continue
            birth_date = self.clinical_birth_dates[clinical_id]
            old_criterion_matches = op_func(birth_date, old_criterion_val)
            new_criterion_matches = op_func(birth_date, new_criterion_val)
            if old_criterion_matches != new_criterion_matches:
                ids_aged.add(clinical_id)

        return ids_aged

    def create_trial_matches(self, trial_match: TrialMatch) -> List[Dict]:
        """Stub function to be overriden by plugin"""
        return []

    @property
    def task_q(self):
        return self._task_q

    @property
    def loop(self):
        return self._loop

    @property
    def matches(self):
        return self._matches

    def get_extra_field_mapping(self, raw_mapping: Dict[ObjectId, Any], key: str) -> Dict[Any: Set[
        ObjectId]]:
        fields = self.config.get("extra_initial_mapping_fields", dict()).get(key, list())
        mapping = defaultdict(lambda: defaultdict(set))
        for obj_id, raw_map in raw_mapping.items():
            for field_name, field_transform in fields:
                field_value = raw_map.get(field_name)
                if field_transform == "date":
                    if field_value.__class__ is not datetime.datetime:
                        try:
                            field_value = dateutil.parser.parse(raw_map.get(field_name))
                        except ValueError:
                            field_value = None
                mapping[field_name][field_value].add(obj_id)
        return mapping

    def get_extra_field_lookup(self, raw_mapping: Dict[ObjectId, Any], key: str) -> Dict[Any: Set[ObjectId]]:
        fields = self.config.get("extra_initial_lookup_fields", dict()).get(key, list())
        mapping = defaultdict(dict)
        for obj_id, raw_map in raw_mapping.items():
            for field_name, field_transform in fields:
                field_value = raw_map.get(field_name)
                if field_transform == "date":
                    if field_value.__class__ is not datetime.datetime:
                        try:
                            field_value = dateutil.parser.parse(raw_map.get(field_name))
                        except (ValueError, TypeError):
                            field_value = None
                if field_value is not None:
                    mapping[field_name][obj_id] = field_value
        return mapping

    def drop_existing_matches(self, protocol_nos: List[str] = None, sample_ids: List[str] = None):
        drop_query = dict()
        if protocol_nos is not None:
            drop_query.update({self.match_criteria_transform.trial_identifier: {'$in': protocol_nos}})
        if sample_ids is not None:
            drop_query.update({'sample_id': {'$in': sample_ids}})
        if protocol_nos is None and sample_ids is None:
            self.db_rw.get_collection(self.trial_match_collection).drop()
        else:
            self.db_rw.get_collection(self.trial_match_collection).remove(drop_query)

    @property
    def drop(self):
        return self._drop
