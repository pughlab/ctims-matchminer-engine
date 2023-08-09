from __future__ import annotations

from typing import (
    NewType,
    Tuple,
    Union,
    List,
    Dict,
    Any,
)

from bson import ObjectId
from networkx import DiGraph

from matchengine.internals.utilities.object_comparison import nested_object_hash

from dataclasses import dataclass
from pymongo.results import BulkWriteResult

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", DiGraph)
NodeID = NewType("NodeID", int)
MatchClauseLevel = NewType("MatchClauseLevel", str)
MongoQueryResult = NewType("MongoQueryResult", Dict[str, Any])
MongoQuery = NewType("MongoQuery", Dict[str, Any])
GenomicID = NewType("GenomicID", ObjectId)
ClinicalID = NewType("ClinicalID", ObjectId)
Collection = NewType("Collection", str)

@dataclass
class UpdateResult:
    inserted_count: int = 0
    modified_count: int = 0

    def add_bulk_write_result(self, bwr: BulkWriteResult):
        self.inserted_count += bwr.inserted_count + bwr.upserted_count
        self.modified_count += bwr.modified_count

    def fmt(self):
        return f"{self.inserted_count} records inserted, {self.modified_count} modified"

class PoisonPill(object):
    __slots__ = ()


class CheckIndicesTask(object):
    __slots__ = ()


class IndexUpdateTask(object):
    __slots__ = (
        "collection", "index"
    )

    def __init__(
            self,
            collection: str,
            index: str
    ):
        self.index = index
        self.collection = collection


class QueryTask(object):
    __slots__ = (
        "trial", "match_clause_data", "match_path",
        "query"
    )

    def __init__(
            self,
            trial: Trial,
            match_clause_data: MatchClauseData,
            match_path: MatchCriterion,
            query: MultiCollectionQuery,
    ):
        self.query = query
        self.match_path = match_path
        self.match_clause_data = match_clause_data
        self.trial = trial


class UpdateTask(object):
    __slots__ = (
        "ops", "protocol_no"
    )

    def __init__(
            self,
            ops: List,
            protocol_no: str
    ):
        self.ops = ops
        self.protocol_no = protocol_no


class RunLogUpdateTask(object):
    __slots__ = (
        "protocol_no"
    )

    def __init__(
            self,
            protocol_no: str
    ):
        self.protocol_no = protocol_no


Task = NewType("Task", Union[PoisonPill, CheckIndicesTask, IndexUpdateTask, QueryTask, UpdateTask, RunLogUpdateTask])


class MatchCriteria(object):
    __slots__ = (
        "criteria", "depth", "node_id"
    )

    def __init__(
            self,
            criteria: Dict,
            depth: int,
            node_id: int
    ):
        self.criteria = criteria
        self.depth = depth
        self.node_id = node_id


class MatchCriterion(object):
    __slots__ = (
        "criteria_list", "_hash"
    )

    def __init__(
            self,
            criteria_list: List[MatchCriteria]
    ):
        self.criteria_list = criteria_list
        self._hash = None

    def add_criteria(self, criteria: MatchCriteria):
        self._hash = None
        self.criteria_list.append(criteria)

    def hash(self) -> str:
        if self._hash is None:
            self._hash = nested_object_hash({"query": [criteria.criteria for criteria in self.criteria_list]})
        return self._hash


class QueryPart(object):
    __slots__ = (
        "render",
        "negate",
        "_query",
    )

    def __init__(
            self,
            query: Dict,
            negate: bool,
            render: bool,
    ):
        self.render = render
        self.negate = negate
        self._query = query


    def set_query_attr(
            self,
            key,
            value
    ):
        self._query[key] = value

    def __copy__(self):
        qp = QueryPart(
            self.query,
            self.negate,
            self.render,
        )
        return qp

    @property
    def query(self):
        return self._query


class QueryNode(object):
    __slots__ = (
        "trial_key", "_query_parts",
         "is_finalized", "_hash",
        "_raw_query", "_raw_query_hash",
        "node_id", 
        "_query_parts_by_key", "raw_collection",
        "raw_join_field", "raw_id_field"
    )

    def __init__(
            self,
            query_parts: List[QueryPart],
            raw_collection: str,
            raw_join_field: str,
            raw_id_field: str
    ):

        self._query_parts = query_parts
        self.is_finalized = False
        self.raw_collection = raw_collection
        self.raw_join_field = raw_join_field
        self.raw_id_field = raw_id_field
        self._raw_query = None
        self._raw_query_hash = None
        self._query_parts_by_key = None


    def is_empty(self):
        return all(
            (not qp.render)
            for qp in self._query_parts
        )

    def add_query_part(self, query_part: QueryPart):
        if self.is_finalized:
            raise Exception("Query node is finalized")
        else:
            self._query_parts.append(query_part)

    def _extract_raw_query(self):
        result = {}
        for query_part in self._query_parts:
            if query_part.render:
                for k, v in query_part.query.items():
                    if k in result:
                        raise ValueError("Generated query with duplicate keys")
                    result[k] = v
        return result

    def extract_raw_query(self):
        if self.is_finalized:
            return self._raw_query
        else:
            return self._extract_raw_query()

    def raw_query_hash(self):
        if not self.is_finalized:
            raise Exception("Query node is not finalized")
        elif self._raw_query_hash is None:
            self._raw_query_hash = nested_object_hash(self._raw_query)
        return self._raw_query_hash

    def finalize(self):
        self.is_finalized = True
        self._raw_query = self._extract_raw_query()
        self._query_parts_by_key = {}
        for qp in self._query_parts:
            for k in qp.query:
                self._query_parts_by_key[k] = qp

    def get_query_part_by_key(self, key: str) -> QueryPart:
        if not self.is_finalized:
            for qp in self._query_parts:
                if key in qp.query:
                    return qp
        else:
            return self._query_parts_by_key.get(key)

    def get_query_part_value_by_key(self, key: str, default: Any = None) -> Any:
        query_part = self.get_query_part_by_key(key)
        if query_part is not None:
            return query_part.query.get(key, default)


class QueryNodeContainer(object):
    __slots__ = (
        "query_nodes",
        "trial_key",
        "trial_value",
        "exclusion",
        "node_depth"
    )

    def __init__(
            self,
            query_nodes: List[QueryNode],
            trial_key: str,
            trial_value: dict,
            node_depth: int,
            exclusion: bool
    ):
        self.query_nodes = query_nodes
        self.trial_key = trial_key
        self.trial_value = trial_value
        self.exclusion = exclusion
        self.node_depth = node_depth



class MultiCollectionQuery(object):
    __slots__ = (
        "query_node_containers"
    )

    def __init__(
            self,
            query_node_containers: List[QueryNodeContainer],
    ):
        self.query_node_containers = query_node_containers


class MatchClauseData(object):
    __slots__ = (
        "match_clause",
        "is_suspended",
        "parent_path", "match_clause_level", "match_clause_additional_attributes",
        "protocol_no"
    )

    def __init__(self,
                 match_clause: MatchClause,
                 is_suspended: bool,
                 parent_path: ParentPath,
                 match_clause_level: MatchClauseLevel,
                 match_clause_additional_attributes: dict,
                 protocol_no: str):
        self.is_suspended = is_suspended
        self.parent_path = parent_path
        self.match_clause_level = match_clause_level
        self.match_clause_additional_attributes = match_clause_additional_attributes
        self.protocol_no = protocol_no
        self.match_clause = match_clause


class MatchReason(object):
    __slots__ = (
        "clinical_id",
        "depth",
        "query",
        "query_kind",
        "reference_docs",
        "exclusion",
    )

    def __init__(
            self,
            query_node_container: QueryNodeContainer,
            exclusion: bool,
            reference_docs: List[dict],
    ):
        # CTML:
        self.query_kind = query_node_container.trial_key # e.g. "clinical," "genomic"
        self.query = query_node_container.trial_value # original CTML query
        self.depth = query_node_container.node_depth

        # Reason information:
        self.reference_docs = reference_docs
        self.exclusion = exclusion


class TrialMatch(object):
    __slots__ = (
        "trial", "match_clause_data", "match_criterion_hash",
        "match_clause_data", "match_reasons",
        "clinical_doc",
        "is_suspended", "match_clause_path",
        "match_clause_level", "match_clause_parent",
        "match_clause", "trial_closed"
    )

    def __init__(
            self,
            trial: Trial,
            match_clause_data: MatchClauseData,
            match_criterion: MatchCriterion,
            match_reasons: List[MatchReason],
            clinical_doc: dict,
            trial_closed: bool
    ):
        self.clinical_doc = clinical_doc
        self.match_reasons = match_reasons

        self.trial = trial
        self.trial_closed = trial_closed
        self.match_clause_path = match_clause_data.parent_path
        self.is_suspended = match_clause_data.is_suspended
        self.match_clause_level = match_clause_data.match_clause_level
        self.match_clause_parent = match_clause_data.match_clause_additional_attributes
        self.match_clause = match_clause_data.match_clause
        self.match_criterion_hash = match_criterion.hash()




class Cache(object):
    def __init__(self):
        self.query_results = {}
        self.query_tasks = {}
        self.doc_results = {}
        self.doc_tasks = {}


class Secrets(object):
    __slots__ = (
        "HOST", "PORT", "DB",
        "AUTH_DB", "RO_USERNAME", "RO_PASSWORD",
        "RW_USERNAME", "RW_PASSWORD", "REPLICA_SET",
        "MAX_POOL_SIZE", "MIN_POOL_SIZE"
    )

    def __init__(
            self,
            host: str,
            port: int,
            db: str,
            auth_db: str,
            ro_username: str,
            ro_password: str,
            rw_username: str,
            rw_password: str,
            replica_set: str,
            max_pool_size: str,
            min_pool_size: str
    ):
        self.MIN_POOL_SIZE = min_pool_size
        self.MAX_POOL_SIZE = max_pool_size
        self.REPLICA_SET = replica_set
        self.RW_PASSWORD = rw_password
        self.RW_USERNAME = rw_username
        self.RO_PASSWORD = ro_password
        self.RO_USERNAME = ro_username
        self.AUTH_DB = auth_db
        self.DB = db
        self.PORT = port
        self.HOST = host


class QueryTransformerResult(object):
    __slots__ = (
        "results"
    )
    results: List[Tuple[Dict, bool]]

    def __init__(
            self,
            query_clause: Dict = None,
            negate: bool = False,
    ):
        self.results = []
        if query_clause is not None:
            self.results.append((query_clause, bool(negate)))

    def add_result(
            self,
            query_clause: Dict,
            negate: bool,
    ):
        self.results.append((query_clause, bool(negate)))
