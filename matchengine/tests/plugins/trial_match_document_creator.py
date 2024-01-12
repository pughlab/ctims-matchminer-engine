from typing import Dict

from matchengine.plugin_stub import TrialMatchDocumentCreator
from matchengine.internals.typing.matchengine_types import TrialMatch


class TestTrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> list:
        return []
