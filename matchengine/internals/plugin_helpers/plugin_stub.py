from __future__ import annotations

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from matchengine.internals.match_criteria_transform import (
        MatchCriteriaTransform,
        TransformFunctions
    )
    from matchengine.internals.typing.matchengine_types import (
        Secrets,
        QueryNode,
        TrialMatch,
        Cache,
        QueryNodeContainer,
        ClinicalID, MatchReason
    )
    from typing import (
        Dict,
        NoReturn,
        Set
    )
    from matchengine.internals.engine import MatchEngine


class TrialMatchDocumentCreator(object):
    cache: Cache
    config: Dict
    me: MatchEngine

    def __init__(self, me: MatchEngine):
        self.matchengine = me

    def create_trial_matches(self, trial_match: TrialMatch) -> list:
        return []

class QueryTransformerContainer(object):
    _: MatchCriteriaTransform
    transform: TransformFunctions
    resources: Dict
    resource_paths: Dict


class DBSecrets(object):
    def get_secrets(self) -> Secrets:
        pass


class QueryNodeTransformer(object):
    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
        pass


class QueryNodeClinicalIDsSubsetter(object):
    def extended_query_node_clinical_ids_subsetter(
            self: MatchEngine,
            query_node: QueryNode,
            clinical_ids: Set[ClinicalID]
    ) -> Set[ClinicalID]:
        pass



class QueryNodeContainerTransformer(object):
    def query_container_transform(self: MatchEngine, query_container: QueryNodeContainer):
        pass
