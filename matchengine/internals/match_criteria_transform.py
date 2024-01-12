from __future__ import annotations

import logging
from matchengine.plugin_stub import QueryTransformers

log = logging.getLogger('matchengine.match_criteria_transform')


class MatchCriteriaTransform(object):
    """
    A class used to transform values used in trial curation into values which can be used to query extended_attributes and
    clinical collections.

    For more details and examples, please see the README.
    """

    query_transformers: dict
    config: dict = None
    trial_key_mappings: dict = None
    trial_collection: str = None
    trial_identifier: str = None
    ctml_collection_mappings: dict = None

    def __init__(self, config, transformers: QueryTransformers):
        self.config = config
        self.ctml_collection_mappings = config['ctml_collection_mappings']

        # values used to match extended_attributes/clinical information to trials.
        # For more details and explanation, see the README
        self.projections = {
            collection: {field: 1 for field in fields} for collection, fields in config["projections"].items()
        }

        self.query_transformers = {}
        for attr in dir(transformers):
            if not attr.startswith('_'):
                method = getattr(transformers, attr)
                self.query_transformers[attr] = method

        self.trial_collection = config.get('trial_collection', 'trial')
        self.trial_identifier = config.get('trial_identifier', 'protocol_no')
        self.match_trial_link_id = config.get('match_trial_link_id', self.trial_identifier)
        # By default, only trials that are "Open to Accrual" are run.
        # This value by default is stored inside a "_summary" object.
        # If a different field indicates trial accrual status, that is set here.
        self.use_custom_trial_status_key = self.config.get("trial_status_key", None)
        if self.use_custom_trial_status_key is not None:
            self.custom_status_key_name = self.use_custom_trial_status_key.get("key_name", None)
            self.custom_open_to_accrual_vals = []
            if self.use_custom_trial_status_key.get("open_to_accrual_values", None) is None:
                raise ValueError(
                    "Missing config field: trial_status_key.open_to_accrual_values. Must contain list of "
                    "acceptable 'open to accrual' values"
                )

            # be case insensitive when checking trial open/close status
            for val in self.use_custom_trial_status_key.get("open_to_accrual_values", None):
                if isinstance(val, str):
                    self.custom_open_to_accrual_vals.append(val.lower().strip())
                else:
                    self.custom_open_to_accrual_vals.append(val)
