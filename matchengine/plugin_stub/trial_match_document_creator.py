
from typing import List
from matchengine.internals.typing.matchengine_types import TrialMatch

class TrialMatchDocumentCreator(object):
    """
    A TrialMatchDocumentCreator creates a set of trial match documents
    for a given "match path," i.e. a combination of QueryNodeContainers ANDed
    together into a query. The match path information is consolidated into
    a TrialMatch object before processing.
    """

    def create_trial_matches(self, trial_match: TrialMatch) -> List[dict]:
        return [ {} ]
