from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING
from matchengine.internals.utilities.object_comparison import nested_object_hash

from matchengine.internals.plugin_helpers.plugin_stub import TrialMatchDocumentCreator

if TYPE_CHECKING:
    from matchengine.internals.typing.matchengine_types import TrialMatch, MatchReason, QueryNode
    from typing import Dict


class DFCITrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> list:
        """
        Create trial match documents based on a TrialMatch, the result of matching a particular
        match path (i.e. a particular set of queries) against a given sample.
        """

        results = []
        for match_reason in trial_match.match_reasons:
            if not self._is_valid_reason(match_reason):
                continue
            match_doc = self._create_trial_match(trial_match, match_reason)
            results.append(match_doc)

        show_in_ui = True
        for reason in trial_match.match_reasons:
            if (
                reason.reason_name == "genomic"
                and reason.query_node.get_query_part_by_key('STRUCTURAL_VARIANT_COMMENT') is not None
                and not reason.query_node.exclusion
            ):
                show_in_ui = False

        for match_doc in results:
            match_doc['show_in_ui'] = show_in_ui

        for match_doc in results:
            match_doc["sort_order"] = self._get_sort_order(match_doc)
        return results

    def _is_valid_reason(self, reason: MatchReason):
        if reason.reason_name == "clinical":
            if "TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE" in reason.query_part.query:
                return True
            else:
                return False
        else:
            return True

    def _get_sort_order(self, match_document: Dict) -> list:
        """
        Sort trial matches based on sorting order specified in config.json under the key 'trial_match_sorting'.

        The function will iterate over the objects in the 'trial_match_sorting', and then look for that value
        in the trial_match document, placing it in an array.

        If being displayed, the matchminerAPI filters the array to output a single sort number.

        The sorting is currently organized as follows:
        1. MMR status
        2. Tumor Mutational Burden
        3. UVA/POLE/APOBEC/Tobacco Status
        4. Tier 1
        5. Tier 2
        6. CNV
        7. Tier 3
        8. Tier 4
        9. wild type
        10. Variant Level
        11. Gene-level
        12. Exact cancer match
        13. General cancer match (all solid/liquid)
        14. DFCI Coordinating Center
        15. All other Coordinating centers
        16. Protocol Number
        """
        sort_map = self.TRIAL_MATCH_SORTING
        sort_array = list()

        for sort_dimension in sort_map:
            sort_index = 99
            for sort_key in sort_dimension:
                if sort_key in match_document:
                    sorting_vals = sort_dimension[sort_key]
                    is_any = sorting_vals.get("ANY_VALUE", None)
                    trial_match_val = str(match_document[sort_key]) if is_any is None else "ANY_VALUE"

                    if (trial_match_val is not None and trial_match_val in sorting_vals) or is_any is not None:
                        matched_sort_int = sort_dimension[sort_key][trial_match_val]
                        if matched_sort_int < sort_index:
                            sort_index = matched_sort_int

            sort_array.append(sort_index)

        # If an idenfitifer is not a protocol id (e.g. 17-251) then skip replacing
        identifier = match_document.get("protocol_no", None)
        sort_array.append(int(identifier.replace("-", "")))

        return sort_array

    def _create_trial_match(self, trial_match: TrialMatch, match_reason: MatchReason) -> Dict:
        """
        Create a trial match document to be inserted into the db. Add clinical, extended_attributes, and trial details as specified
        in config.json
        """

        clinical_doc = self.matchengine.cache.docs[match_reason.clinical_id]

        trial_status = trial_match.trial.get('_summary', dict()).get('status', [dict()])
        site_status = trial_status[0].get('value', 'open to accrual').lower()
        status_for_match_clause = 'open' if site_status.lower() == 'open to accrual' else 'closed'
        coordinating_center = trial_match.trial.get('_summary', dict()).get('coordinating_center', 'unknown')

        new_trial_match = {
            'clinical_id': match_reason.clinical_id,
            'match_level': trial_match.match_clause_data.match_clause_level,
            'internal_id': trial_match.match_clause_data.internal_id,
            'reason_type': match_reason.reason_name,
            'q_depth': match_reason.depth,
            'q_width': match_reason.width,
            'code': trial_match.match_clause_data.code,
            'trial_curation_level_status': 'closed' if trial_match.match_clause_data.is_suspended else 'open',
            'trial_summary_status': status_for_match_clause,
            'coordinating_center': coordinating_center,
            'query_hash': trial_match.match_criterion.hash(),
            'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path]),
        }

        new_trial_match.update(
            {
                k.lower(): v
                for k, v in clinical_doc.items()
                if k
                in {
                    "REPORT_DATE",
                    "GENDER",
                    "SAMPLE_ID",
                    "MRN",
                    "ONCOTREE_PRIMARY_DIAGNOSIS_NAME",
                    "TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE",
                    "VITAL_STATUS",
                }
            }
        )
        # add trial fields except for extras
        new_trial_match.update({k: v for k, v in trial_match.trial.items() if k in {'protocol_no', 'nct_id'}})
        new_trial_match['combo_coord'] = nested_object_hash(
            {
                'query_hash': new_trial_match['query_hash'],
                'match_path': new_trial_match['match_path'],
                'protocol_no': new_trial_match['protocol_no'],
            }
        )

        new_trial_match.update({'cancer_type_match': self._get_cancer_type_match(trial_match)})

        if match_reason.reason_name == 'genomic':
            if match_reason.query_node.exclusion:
                new_trial_match.update(self._format_exclusion_match(match_reason.query_node))
            else:
                genomic_doc = self.matchengine.cache.docs[match_reason.reference_id]
                new_trial_match.update(
                    {
                        k.lower(): v
                        for k, v in genomic_doc.items()
                        if k
                        in {
                            "TRUE_CDNA_CHANGE",
                            "HARMONIZED_CDNA_CHANGE",
                            "REFERENCE_ALLELE",
                            "ALLELE_FRACTION",
                            "STRUCTURAL_VARIANT_COMMENT",
                            "SAMPLE_ID",
                            "CLINICAL_ID",
                            "VARIANT_CATEGORY",
                            "WILDTYPE",
                            "TRUE_TRANSCRIPT_EXON",
                            "HARMONIZED_TRANSCRIPT_EXON",
                            "TIER",
                            "TRUE_HUGO_SYMBOL",
                            "HARMONIZED_HUGO_SYMBOL",
                            "TRUE_PROTEIN_CHANGE",
                            "HARMONIZED_PROTEIN_CHANGE",
                            "CNV_CALL",
                            "TRUE_VARIANT_CLASSIFICATION",
                            "HARMONIZED_VARIANT_CLASSIFICATION",
                            "MMR_STATUS",
                            "STRUCTURAL_VARIANT_COMMENT",
                            "CHROMOSOME",
                            "ACTIONABILITY",
                            "POSITION",
                            "APOBEC_STATUS",
                            "POLE_STATUS",
                            "TOBACCO_STATUS",
                            "TEMOZOLOMIDE_STATUS",
                            "UVA_STATUS",
                            "LEFT_PARTNER_GENE",
                            "HARMONIZED_LEFT_PARTNER_GENE",
                            "RIGHT_PARTNER_GENE",
                            "HARMONIZED_RIGHT_PARTNER_GENE",
                            "STRUCTURAL_VARIANT_TYPE",
                        }
                    }
                )
                new_trial_match.update(self._get_genomic_details(genomic_doc, match_reason.query_node))
        elif match_reason.reason_name == 'clinical':
            query = match_reason.extract_raw_query()
            new_trial_match.update(self._get_clinical_details(clinical_doc, query))

        return new_trial_match

    def _get_genomic_details(self, genomic_doc: Dict, query_node: QueryNode):
        alteration = list()

        wildtype = genomic_doc.get('WILDTYPE', None)
        protein_change = genomic_doc.get('HARMONIZED_PROTEIN_CHANGE', None)
        hugo_symbol = genomic_doc.get('HARMONIZED_HUGO_SYMBOL', None)
        cnv = genomic_doc.get('CNV_CALL', None)
        variant_classification = genomic_doc.get("HARMONIZED_VARIANT_CLASSIFICATION", None)
        variant_category = genomic_doc.get('VARIANT_CATEGORY', None)
        criteria_ancestor = query_node.criterion_ancestor[query_node.query_level]
        is_variant = 'gene'

        # add wildtype calls
        if wildtype:
            alteration.append('wt ')

        # add gene
        if hugo_symbol is not None:
            alteration.append(hugo_symbol)

        # add mutation
        if protein_change is not None:
            alteration.append(f' {protein_change}')
            is_variant = (
                'variant'
                if {'protein_change', 'wildcard_protein_change'}.intersection(set(criteria_ancestor.keys()))
                else 'gene'
            )

        # add cnv call
        elif cnv:
            alteration.append(f' {cnv}')

        # add variant classification
        elif variant_classification:
            alteration.append(f' {variant_classification}')

        # add structural variation
        elif variant_category == 'SV':
            genomic_left = genomic_doc.get("HARMONIZED_LEFT_PARTNER_GENE", False)
            genomic_right = genomic_doc.get("HARMONIZED_RIGHT_PARTNER_GENE", False)
            if (genomic_left is not False) or (genomic_right is not False):
                criteria_left = criteria_ancestor.get("hugo_symbol", str()).lower().replace(" ", "_")
                criteria_right = criteria_ancestor.get("fusion_partner_hugo_symbol", str()).lower().replace(" ", "_")
                is_variant = (
                    'variant'
                    if criteria_left not in {'', 'any_gene'} and criteria_right not in {'', 'any_gene'}
                    else 'gene'
                )
                if genomic_left and genomic_right:
                    alteration.append(f'{genomic_left}-{genomic_right}')
                else:
                    alteration.append(f'{genomic_left or genomic_right}-intergenic')
                structural_variant_type = genomic_doc.get('STRUCTURAL_VARIANT_TYPE', None)
                alteration.append(' ')
                alteration.append('Structural Variant' if structural_variant_type is None else structural_variant_type)
            else:
                query = query_node.extract_raw_query()
                sv_comment = query.get('STRUCTURAL_VARIANT_COMMENT', None)
                pattern = sv_comment.pattern.split("|")[0] if sv_comment is not None else None
                gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "") if pattern is not None else None
                alteration.append(f'{gene} Structural Variation' if gene else 'Structural Variation')

        # add mutational signature
        elif variant_category == 'SIGNATURE':
            query = query_node.extract_raw_query()
            signature_type = next(
                chain(
                    {
                        'UVA_STATUS',
                        'TOBACCO_STATUS',
                        'POLE_STATUS',
                        'TEMOZOLOMIDE_STATUS',
                        'MMR_STATUS',
                        'APOBEC_STATUS',
                    }.intersection(query.keys())
                )
            )
            signature_value = genomic_doc.get(signature_type, None)
            is_variant = "signature"
            if signature_type == 'MMR_STATUS':
                is_variant = 'mmr'
                mapped_mmr_status = {
                    'Proficient (MMR-P / MSS)': 'MMR-P/MSS',
                    'Deficient (MMR-D / MSI-H)': 'MMR-D/MSI-H',
                }.get(signature_value, None)
                if mapped_mmr_status:
                    alteration.append(mapped_mmr_status)
            elif signature_type is not None:
                signature_type = signature_type.replace('_STATUS', ' Signature')
                signature_type = {
                    'TEMOZOLOMIDE Signature': 'Temozolomide Signature',
                    'TOBACCO Signature': 'Tobacco Signature',
                }.get(signature_type, signature_type)
                alteration.append(f'{str() if signature_value.lower() == "yes" else "No "}' f'{signature_type}')
        return {
            'match_type': is_variant,
            'genomic_alteration': ''.join(alteration),
            'genomic_id': genomic_doc['_id'],
        }

    def _get_clinical_details(self, clinical_doc, query):
        c_tmb, q_tmb = map(lambda x: x.get("TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE", None), (clinical_doc, query))
        if all((q_tmb, c_tmb)):
            return {
                'match_type': "tmb",
                'genomic_alteration': f"TMB = {c_tmb}",
                'variant_category': 'TMB',
            }
        else:
            return {
                'match_type': 'generic_clinical',
                'genomic_alteration': '',
            }

    def _format_exclusion_match(self, query_node: QueryNode):
        """Format the extended_attributes alteration for extended_attributes documents that matched a negative clause of a match tree"""

        query = query_node.extract_raw_query()

        hugo_symbol = 'HARMONIZED_HUGO_SYMBOL'
        protein_change_key = 'HARMONIZED_PROTEIN_CHANGE'
        cnv_call = 'CNV_CALL'
        variant_classification = 'HARMONIZED_VARIANT_CLASSIFICATION'
        sv_comment = 'STRUCTURAL_VARIANT_COMMENT'
        alteration = ['!']
        is_variant = 'variant' if query.setdefault(protein_change_key, None) is not None else 'gene'

        hugo_symbol_added = False
        if hugo_symbol in query and query[hugo_symbol] is not None:
            alteration.append(f'{query[hugo_symbol]}')
            hugo_symbol_added = True

        # add mutation
        if query.get(protein_change_key, None) is not None:
            if '$regex' in query[protein_change_key]:
                alteration.append(f' {query[protein_change_key]["$regex"].pattern[1:].split("[")[0]}')
            else:
                alteration.append(f' {query[protein_change_key]}')

        # add cnv call
        elif query.get(cnv_call, None) is not None:
            alteration.append(f' {query[cnv_call]}')

        # add variant classification
        elif query.get(variant_classification, None) is not None:
            alteration.append(f' {query[variant_classification]}')

        # add structural variation
        elif query.get(sv_comment, None) is not None:
            pattern = query[sv_comment].pattern.split("|")[0]
            gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "")
            alteration.append(f'{gene} Structural Variation')

        else:
            criteria = query_node.criterion_ancestor[query_node.query_level]
            if criteria.get('variant_category', str()).lower() == '!structural variation':
                left = criteria.get("hugo_symbol", '')
                right = criteria.get("fusion_partner_hugo_symbol", '')
                is_variant = 'variant' if left not in {'', 'any_gene'} and right not in {'', 'any_gene'} else 'gene'

                alteration.append((f'{left}' f'{"-" if left and right else ""}' f'{right}' ' Structural Variation'))

        if len(alteration) == 2 and hugo_symbol_added:
            alteration.append(' Mutation')
        return {'match_type': is_variant, 'genomic_alteration': ''.join(alteration)}

    def _get_cancer_type_match(self, trial_match):
        """Trial curations with _SOLID_ and _LIQUID_ should report those as reasons for match. All others should report
        'specific'"""
        cancer_type_match = 'specific'
        for criteria in trial_match.match_criterion.criteria_list:
            for node in criteria.criteria:
                if 'clinical' in node and 'oncotree_primary_diagnosis' in node['clinical']:
                    diagnosis = node['clinical']['oncotree_primary_diagnosis']
                    if diagnosis == '_LIQUID_':
                        cancer_type_match = 'all_liquid'
                        break
                    elif diagnosis == '_SOLID_':
                        cancer_type_match = 'all_solid'
                        break
        return cancer_type_match

    TRIAL_MATCH_SORTING = [
        {"show_in_ui": {"True": 1, "False": -1}, "trial_curation_level_status": {"closed": -1, "open": 1}},
        {
            "match_type": {"mmr": 20, "tmb": 30},
            "temozolomide_status": {"Yes": 40},
            "apobec_status": {"Yes": 40},
            "tobacco_status": {"Yes": 40},
            "pole_status": {"Yes": 40},
            "uva_status": {"Yes": 40},
            "tier": {"1": 50, "2": 60, "3": 80, "4": 90},
            "cnv_call": {
                "Homozygous deletion": 70,
                "High level amplification": 71,
                "Gain": 72,
                "Heterozygous deletion": 73,
            },
            "wildtype": {"False": 100, "True": 100},
        },
        {"match_type": {"variant": 0, "gene": 1}},
        {"variant_category": {"SV": 0}},
        {"cancer_type_match": {"_SOLID_": 100, "_LIQUID_": 100}},
        {"coordinating_center": {"Dana-Farber Cancer Institute": 0}},
    ]


__export__ = ["DFCITrialMatchDocumentCreator"]
