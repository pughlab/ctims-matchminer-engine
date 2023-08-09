
from matchengine.internals.typing.matchengine_types import QueryNode

class ClinicalFilter:
    def should_match(
        self,
        clinical_doc: dict
    ) -> bool:
        """
        Determines whether a specific clinical record should be matched,
        based on prefetched data from the clinical collection.
        If this returns false, it will be assumed that the sample is not a match.
        """
        return True

    def should_filter(
        self,
        query_node: QueryNode
    ) -> bool:
        """
        Determines whether filtering (as defined by apply_filter below)
        should occur for the given query node.
        """
        return False

    def apply_filter(
        self,
        query_node: QueryNode,
        clinical_doc: dict
    ) -> bool:
        """
        Determines whether a specific query should be run for a given
        clinical ID, based on prefetched data from the clinical collection.
        """
        return True
