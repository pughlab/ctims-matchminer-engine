from __future__ import annotations

import re
import json

from matchengine.internals.typing.matchengine_types import QueryTransformerResult
from matchengine.plugin_stub import QueryTransformers
from pathlib import Path

class CustomQueryTransformers(QueryTransformers):
    def __init__(self):
        super().__init__()
        try:
            oncotree_file = Path(__file__).parent / 'oncotree_mapping.json'
            with oncotree_file.open('r') as file:
                oncotree = json.load(file)
            oncotree = {
                key:
                [val] if not isinstance(val, list) else sorted(val)
                for key, val in oncotree.items()
            }
            self._oncotree = oncotree
        except Exception as e:
            raise Exception("failed to load oncotree file") from e

    def tmb_range_to_query(self, sample_key, trial_value, **kwargs):
        operator_map = {
            "==": "$eq",
            "<=": "$lte",
            ">=": "$gte",
            ">": "$gt",
            "<": "$lt"
        }
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        if numeric.startswith('.'):
            numeric = '0' + numeric
        return QueryTransformerResult({sample_key: {operator_map[operator]: float(numeric)}}, False)

    def oncotree_map(self, sample_key, trial_value, **kwargs):
        trial_value, negate = self._is_negate(trial_value)
        return QueryTransformerResult({sample_key: {"$in": self._oncotree.get(trial_value, trial_value)}}, negate)

    def cnv_map(self, sample_key, trial_value, **kwargs):
        # Heterozygous deletion,
        # Gain,
        # Homozygous deletion,
        # High level amplification,
        # Neu
        cnv_map = {
            "High Amplification": "High level amplification",
            "Homozygous Deletion": "Homozygous deletion",
            'Low Amplification': 'Gain',
            'Heterozygous Deletion': 'Heterozygous deletion'
        }

        trial_value, negate = self._is_negate(trial_value)
        if trial_value in cnv_map:
            return QueryTransformerResult({sample_key: cnv_map[trial_value]}, negate)
        else:
            return QueryTransformerResult({sample_key: trial_value}, negate)

    def variant_category_map(self, sample_key, trial_value, **kwargs):
        variant_category_map = {
            "Copy Number Variation".lower(): "CNV",
            "Any Variation".lower(): {"$in": ["MUTATION", "CNV"]},
            "Structural Variation".lower(): "SV"
        }

        trial_value, negate = self._is_negate(trial_value)

        # if a curation calls for a Structural Variant, search the free text in the extended_attributes document under
        # STRUCTURAL_VARIANT_COMMENT for mention of the TRUE_HUGO_SYMBOL
        if trial_value == 'Structural Variation':
            sample_value = variant_category_map.get(trial_value.lower())
            results = QueryTransformerResult()
            results.add_result({'STRUCTURAL_VARIANT_COMMENT': None, sample_key: sample_value}, negate)
            results.add_result({'STRUCTURED_SV': None, sample_key: sample_value}, negate)
            return results
        elif trial_value.lower() in variant_category_map:
            return QueryTransformerResult({sample_key: variant_category_map[trial_value.lower()]}, negate)
        else:
            return QueryTransformerResult({sample_key: trial_value.upper()}, negate)

    def wildcard_regex(self, sample_key, trial_value, **kwargs):
        """
        When trial curation criteria include a wildcard prefix (e.g. WILDCARD_PROTEIN_CHANGE), a extended_attributes query must
        use a $regex to search for all extended_attributes documents which match the protein prefix.

        E.g.
        Trial curation match clause:
        | extended_attributes:
        |    wildcard_protein_change: p.R132

        Patient extended_attributes data:
        |    true_protein_change: p.R132H

        The above should match in a mongo query.
        """
        # By convention, all protein changes being with "p."

        trial_value, negate = self._is_negate(trial_value)
        if not trial_value.startswith('p.'):
            trial_value = re.escape('p.' + trial_value)
        trial_value = f'^{trial_value}[ACDEFGHIKLMNPQRSTVWY]$'
        return QueryTransformerResult({sample_key: {'$regex': re.compile(trial_value, re.IGNORECASE)}},
                                      negate)

    def mmr_ms_map(self, sample_key, trial_value, **kwargs):
        mmr_map = {
            'MMR-Proficient': 'Proficient (MMR-P / MSS)',
            'MMR-Deficient': 'Deficient (MMR-D / MSI-H)',
            'MSI-H': 'Deficient (MMR-D / MSI-H)',
            'MSI-L': 'Proficient (MMR-P / MSS)',
            'MSS': 'Proficient (MMR-P / MSS)'
        }
        trial_value, negate = self._is_negate(trial_value)
        sample_value = mmr_map[trial_value]
        return QueryTransformerResult({sample_key: sample_value}, negate)
