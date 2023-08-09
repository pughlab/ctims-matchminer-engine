
from matchengine.internals.typing.matchengine_types import QueryNodeContainer

class QueryProcessor(object):
    def transform(self, query: QueryNodeContainer):
        """
        Postprocessees a QueryNodeContainer, the result of transforming
        a specific set of genomic or clinical criteria.
        """
        pass
