from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from matchengine.internals.utilities.object_comparison import nested_object_hash

import dateutil.parser
import datetime
from matchengine.plugin_stub import TrialMatchDocumentCreator
import json

if TYPE_CHECKING:
    from matchengine.internals.typing.matchengine_types import TrialMatch, MatchReason
    from typing import Dict


class PughLabTrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> list:
        """
        Create trial match documents based on a TrialMatch, the result of matching a particular
        match path (i.e. a particular set of queries) against a given sample.
        """

        # We generate one document for each matching exclusion criterion
        # and one document for each genomic document matching an inclusion criterion.
        # Custom plugins may want to consolidate this.

        # Generate the "base" part shared between all documents generated.
        results = []
        base_match_doc = self._render_trial_match(trial_match)

        # Generate the parts specific to each document or exclusion criterion.
        reason_docs = []
        for reason in trial_match.match_reasons:
            if not reason.exclusion:
                for rdoc in reason.reference_docs:
                    reason_doc = self._render_inclusion_match(reason, rdoc)
                    reason_docs.append(reason_doc)
            else:
                reason_doc = self._render_exclusion_match(reason, trial_match.clinical_doc)
                reason_docs.append(reason_doc)

        # Eliminate "generic" reasons; ensure we still produce at least one document, though.
        reason_docs = [
            reason_doc
            for reason_doc in reason_docs
            if (reason_doc.get('match_type') != 'generic_clinical')
        ]
        if not reason_docs:
            reason_docs = [{}]

        # Combine these to produce the results.
        for reason_doc in reason_docs:
            match_doc = {}
            match_doc.update(base_match_doc)
            match_doc.update(reason_doc)
            patient_match_values_dict = {}
            for reason in trial_match.match_reasons:
                tv = reason.query
                if reason.query_kind == "genomic":
                    patient_match_values_dict.update({
                        k: v for k, v in reason_doc.items()
                        if k.upper() in self._GENOMIC_COPY_FIELDS and any(k_part in k or k in k_part for k_part in tv.keys())
                    })
                elif reason.query_kind == "clinical":
                    patient_match_values_dict.update({
                        k: v for k, v in base_match_doc.items()
                        if k.upper() in self._CLINICAL_COPY_FIELDS and any(k_part in k or k in k_part for k_part in tv.keys())
                    })
                elif reason.query_kind == "prior_treatment":
                    query_str = reason_doc.get("query")
                    json_compatible_str = query_str.replace("'", '"')
                    query_dict = json.loads(json_compatible_str) 
                    patient_match_values_dict.update({
                        k: v for k, v in query_dict.items()
                        if k.upper() in self._PRIOR_TREATMENT_COPY_FIELDS and any(k_part in k or k in k_part for k_part in tv.keys())
                    })

            patient_match_values_dict.update({'genomic_alteration': reason_doc.get("genomic_alteration", "")})
            # Filter out key-value pairs where the value is an empty string
            filtered_data = {k: v for k, v in patient_match_values_dict.items() if v != "" and v !="NA"}

            # Convert the filtered dictionary to a JSON-like string format
            patient_match_values = f'{filtered_data}'
            match_doc['patient_match_values'] = patient_match_values
            # Try to add all 3 match reason (age, clinical, genomic) together
            query_list = []
            for reason in trial_match.match_reasons:
                query_list.append(reason.query)
            match_doc['queries_used'] = f'{query_list}'
            match_doc["sort_order"] = self._get_sort_order(match_doc)
            results.append(match_doc)

        return results

    def _render_trial_match(self, trial_match: TrialMatch) -> Dict:
        """
        Create a base trial match document with all the fields shared by the documents for each MatchReason.
        """

        coordinating_center = trial_match.trial.get('_summary', {}).get('coordinating_center', 'unknown')

        show_in_ui = True
        for reason in trial_match.match_reasons:
            if reason.query_kind == "genomic" and reason.reference_docs:
                for rdoc in reason.reference_docs:
                    if 'STRUCTURAL_VARIANT_COMMENT' in rdoc:
                        show_in_ui = False

        cancer_type_match = 'specific'
        for reason in trial_match.match_reasons:
            if reason.query_kind == 'clinical':
                tv = reason.query
                diagnosis = tv.get('oncotree_primary_diagnosis')
                if diagnosis == '_LIQUID_':
                    cancer_type_match = 'all_liquid'
                    break
                elif diagnosis == '_SOLID_':
                    cancer_type_match = 'all_solid'
                    break

        # get the drug names for the match using trial ctml info and trial_match path
        drug_names = ""
        trial_step_number = ""
        trial_arm_number = ""
        if len(trial_match.match_clause_path) >= 4:
            trial_step_number = trial_match.match_clause_path[2]
            trial_arm_number = trial_match.match_clause_path[4]
            trial_arm_obj = trial_match.trial['treatment_list']['step'][trial_step_number]['arm'][trial_arm_number]
            if 'dose_level' in trial_arm_obj:
                # dose_levels = trial_match.trial['treatment_list']['step'][trial_step_number]['arm'][trial_arm_number]['dose_level']
                dose_levels = trial_arm_obj['dose_level']
            else:
                dose_levels = []

            for level in dose_levels:
                if 'level_code' in level:
                    if drug_names != "":
                        drug_names = drug_names + "," + level['level_code']
                    else:
                        drug_names = drug_names + level['level_code']

        match_parent = trial_match.match_clause_parent
        match_level = trial_match.match_clause_level
        trial_match_doc = {
            'clinical_id': trial_match.clinical_doc['_id'],
            'match_level': self._LEVEL_MAPPING.get(match_level),
            'internal_id': match_parent.get(self._INTERNAL_ID_MAPPING[match_level]),
            'code': match_parent.get(self._CODE_MAPPING[match_level]),
            'trial_curation_level_status': 'closed' if trial_match.is_suspended else 'open',
            'trial_summary_status': 'closed' if trial_match.trial_closed else 'open',
            'coordinating_center': coordinating_center,
            'query_hash': trial_match.match_criterion_hash,
            'match_path': '.'.join([str(item) for item in trial_match.match_clause_path]),
            'cancer_type_match': cancer_type_match,
            'trial_step_number': str(1 + trial_step_number),
            'trial_arm_number': str(1 + trial_arm_number),
            'drug_name': drug_names,
            'trial_id': trial_match.trial['trial_id'],
            'prior_treatment_agent': trial_match.clinical_doc['AGENT'] if 'AGENT' in trial_match.clinical_doc else ''
            # 'show_in_ui': show_in_ui,
        }

        # Add in additional fields we need for frontend
        if ('arm_description' in match_parent):
            trial_match_doc.update({'arm_description': match_parent['arm_description']})

        trial_match_doc.update(
            {k.lower(): v for k, v in trial_match.clinical_doc.items() if k in self._CLINICAL_COPY_FIELDS}
        )
        for reason in trial_match.match_reasons:
            if reason.query_kind == 'clinical':
                tv = reason.query
                diagnosis = tv.get('oncotree_primary_diagnosis_name')
                if diagnosis != 'NONE':
                    actual_match_value = trial_match.clinical_doc.get('ONCOTREE_PRIMARY_DIAGNOSIS_NAME')
                    trial_match_doc.update({'oncotree_primary_diagnosis_match_value': str(actual_match_value)})
            # if reason.query_kind == "prior_treatment":
            #     agents = []
            #     existing_agents = trial_match_doc.get('prior_treatment_agent')
            #     if existing_agents:
            #         agents.append(existing_agents)
            #     if reason.reference_docs:
            #         for rdoc in reason.reference_docs:
            #             agent = rdoc.get("AGENT")
            #             if agent and agent not in agents:
            #                 agents.append(agent)
            #         trial_match_doc.update({'prior_treatment_agent': ",".join(agents)})
            #     elif reason.reference_docs is None and reason.exclusion:
            #         trial_match_doc.update(reason.query)
        # add trial fields except for extras
        trial_match_doc.update({k: v for k, v in trial_match.trial.items() if k in self._TRIAL_COPY_FIELDS})
        trial_match_doc['combo_coord'] = nested_object_hash(
            {
                'query_hash': trial_match_doc['query_hash'],
                'match_path': trial_match_doc['match_path'],
                #'protocol_no': trial_match_doc['protocol_no'],
            }
        )

        return trial_match_doc

    def _render_inclusion_match(self, match_reason: MatchReason, document: dict):
        """
        Create more specific fields for a given MatchReason, which is (here) an inclusion match
        associated with a genomic or clinical record.
        """
        reason_match_doc = {
            'reason_type': match_reason.query_kind,
            'q_depth': match_reason.depth,
            'q_width': len(match_reason.reference_docs),
            'query': f'{match_reason.query}',
        }

        if match_reason.query_kind == 'genomic':
            reason_match_doc.update({k.lower(): v for k, v in document.items() if k in self._GENOMIC_COPY_FIELDS})
            match_type, alteration = self._format_genomic_inclusion_match(match_reason, document)
            reason_match_doc.update(
                {'genomic_id': document['_id'], 'match_type': str(match_type), 'genomic_alteration': str(alteration)}
            )
        elif match_reason.query_kind == 'clinical':
            match_type, alteration = self._format_clinical_inclusion_match(match_reason, document)
            reason_match_doc.update(
                {
                    'match_type': str(match_type),
                    'genomic_alteration': str(alteration),
                }
            )
            if match_type == 'tmb':
                reason_match_doc['variant_category'] = 'TMB'
        elif match_reason.query_kind == 'prior_treatment':
            match_type, alteration = self._format_prior_treatment_match(match_reason, document)
            reason_match_doc.update(
                {
                    'match_type': str(match_type),
                    'prior_treatment_agent': str(alteration),
                }
            )
        return reason_match_doc

    def _render_exclusion_match(self, match_reason: MatchReason, clinical_doc):
        """
        Create more specific fields for a given MatchReason, which is (here) an exclusion match
        associated with a query.
        """
        reason_match_doc = {
            'reason_type': match_reason.query_kind,
            'q_depth': match_reason.depth,
            'q_width': -1,
            'query': f'{match_reason.query}',
        }
        logging.info(f"render exclusion: {match_reason.query_kind}: {match_reason.query}")

        if match_reason.query_kind == 'genomic':
            match_type, alteration = self._format_genomic_exclusion_match(match_reason, clinical_doc)
            reason_match_doc.update({'match_type': str(match_type), 'genomic_alteration': '!' + str(alteration)})
            reason_match_doc['match_type'] = str(match_type)
            reason_match_doc['genomic_alteration'] = '!' + str(alteration)
        elif match_reason.query_kind == 'clinical':
            match_type, alteration = self._format_clinical_exclusion_match(match_reason)
            reason_match_doc.update({'match_type': str(match_type), 'genomic_alteration': '!' + str(alteration)})
        elif match_reason.query_kind == 'prior_treatment':
            match_type, alteration = self._format_prior_treatment_exclusion_match(match_reason)
            reason_match_doc.update({'match_type': str(match_type), 'genomic_alteration': '!' + str(alteration)})

        return reason_match_doc

    def _format_genomic_inclusion_match(self, match_reason: MatchReason, genomic_doc: dict):
        """
        Get match_type and genomic_alteration fields for non-exclusion genomic matches.
        """
        variant_category = genomic_doc.get('VARIANT_CATEGORY')
        query = match_reason.query

        # add mutation/cnv
        if variant_category == 'MUTATION':
            hugo_symbol = genomic_doc.get('TRUE_HUGO_SYMBOL')
            protein_change = genomic_doc.get('TRUE_PROTEIN_CHANGE')
            if protein_change:
                if 'protein_change' in query or 'wildcard_protein_change' in query:
                    return 'variant', f'{hugo_symbol} {protein_change}'
                else:
                    return 'gene', f'{hugo_symbol} {protein_change}'
            variant_classification = genomic_doc.get("TRUE_VARIANT_CLASSIFICATION")
            if variant_classification:
                return 'gene', f'{hugo_symbol} {variant_classification}'
            else:
                return 'gene', f'{hugo_symbol}'
        if variant_category == 'CNV':
            hugo_symbol = genomic_doc.get('TRUE_HUGO_SYMBOL')
            cnv = genomic_doc.get('CNV_CALL')
            if cnv:
                return 'gene', f'{hugo_symbol} {cnv}'
            else:
                return 'gene', f'{hugo_symbol}'
        # add structural variation
        if variant_category == 'SV':
            genomic_left = genomic_doc.get("LEFT_PARTNER_GENE", False)
            genomic_right = genomic_doc.get("RIGHT_PARTNER_GENE", False)
            if (genomic_left is not False) and (genomic_right is not False):
                criteria_left = query.get("hugo_symbol", "").lower().replace(" ", "_")
                criteria_right = query.get("fusion_partner_hugo_symbol", "").lower().replace(" ", "_")
                if criteria_left not in {'', 'any_gene'} and criteria_right not in {'', 'any_gene'}:
                    match_type = 'variant'
                else:
                    match_type = "gene"
                sv_type = genomic_doc.get('STRUCTURAL_VARIANT_TYPE') or 'Structural Variant'
                if genomic_left and genomic_right:
                    return match_type, f'{genomic_left}-{genomic_right} {sv_type}'
                else:
                    return match_type, f'{genomic_left or genomic_right}-intergenic {sv_type}'
            else:
                criteria_left = query.get("hugo_symbol")
                if criteria_left:
                    return 'gene', f'{criteria_left} Structural Variation'
                else:
                    return 'gene', 'Structural Variation'

        # add mutational signature
        if variant_category == 'SIGNATURE':
            if ('mmr_status' in query) or ('ms_status' in query):
                signature_value = genomic_doc.get('MMR_STATUS')
                sig_val = self._MMR_STATUS_DISPLAY_MAP.get(signature_value, signature_value)
                return 'mmr', sig_val
            else:
                signature_types = [
                    (field, display_name)
                    for (ctml_key, (field, display_name)) in self._SIGNATURE_MAP.items()
                    if ctml_key in query
                ]
                alteration_parts = []
                for field, display_name in sorted(signature_types):
                    signature_value = genomic_doc.get(field)
                    match_type = "signature"
                    if signature_value.lower() == "yes":
                        alteration_parts.append(f'{display_name}')
                    else:
                        alteration_parts.append(f'No {display_name}')
                if alteration_parts:
                    return "signature", ", ".join(alteration_parts)
                else:
                    return "signature", "Signature"

        return 'gene', 'Genomic'

    def _format_clinical_inclusion_match(self, match_reason: MatchReason, clinical_doc: dict):
        """
        Get match_type and genomic_alteration fields for non-exclusion clinical matches.
        """
        c_tmb = clinical_doc.get("TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE")
        q_tmb = match_reason.query.get("tmb_numerical")
        if c_tmb and q_tmb:
            return (
                "tmb",
                f"TMB = {c_tmb}",
            )
        else:
            return 'generic_clinical', "Clinical"

    def _format_clinical_exclusion_match(self, match_reason):
        """
        Get match_type and genomic_alteration fields for exclusion clinical matches.
        """
        return 'generic_clinical', "Clinical"

    def _format_prior_treatment_match(self, match_reason: MatchReason, treatment_doc: dict):
        """
        Get match_type and genomic_alteration fields for prior treatment matches.
        """
        agent = treatment_doc.get("AGENT")
        if agent:
            return (
                "prior_treatment_agent",
                f"{agent}",
            )
        else:
            return 'prior_treatment_agent', "prior_treatment_agent"

    def _format_prior_treatment_exclusion_match(self, match_reason: MatchReason):
        """
        Get match_type and genomic_alteration fields for exclusion clinical matches.
        """
        query = match_reason.query
        agent = query.get('prior_treatment_agent')
        if agent:
            return (
                "prior_treatment_agent",
                f"Prior Treatment Agent: {agent}"
            )

    def _fmt_crit(self, trial_value):
        """
        Remove the initial negating '!' signs from CTML criteria to format them for inclusion
        in the genomic_alteration field.
        """
        negate = True if trial_value.__class__ is str and trial_value and trial_value[0] == '!' else False
        trial_value = trial_value[1::] if negate else trial_value
        return str(trial_value) if trial_value else ''

    def _format_genomic_exclusion_match(self, match_reason: MatchReason, clinical_doc):
        """
        Get match_type and genomic_alteration fields for exclusion genomic matches.
        A '!' sign will be prepended to show this as an exclusion match.
        """

        query = match_reason.query

        hugo_symbol = self._fmt_crit(query['hugo_symbol'])

        variant_category = self._fmt_crit(query.get('variant_category')).lower()

        # add mutation
        if query.get('wildcard_protein_change'):
            return 'variant', f'{hugo_symbol} {self._fmt_crit(query["wildcard_protein_change"])}'
        elif query.get('protein_change'):
            return 'variant', f'{hugo_symbol} {self._fmt_crit(query["protein_change"])}'
        # add cnv call
        elif query.get('cnv_call'):
            cnv_call = self._fmt_crit(query['cnv_call'])
            cnv_map = self._CNV_CALL_DISPLAY_MAP
            return 'gene', f'{hugo_symbol} {cnv_map.get(cnv_call, cnv_call)}'
        # add variant classification
        elif query.get('variant_classification'):
            return 'gene', f'{hugo_symbol} {self._fmt_crit(query["variant_classification"])}'
        # add structural variation
        elif variant_category == 'structural variation':
            left = self._fmt_crit(query.get("hugo_symbol"))
            right = self._fmt_crit(query.get("fusion_partner_hugo_symbol"))
            if self._would_be_unstructured(clinical_doc):
                return 'gene', f'{left} Structural Variation'
            else:
                match_type = 'variant' if (left not in {'', 'any_gene'} and right not in {'', 'any_gene'}) else 'gene'
                if left and right:
                    return match_type, f'{left}-{right} Structural Variation'
                else:
                    return match_type, f'{left or right} Structural Variation'
        else:
            return 'gene', f'{hugo_symbol} Mutation'

    def _would_be_unstructured(self, clinical_doc):
        report_date = clinical_doc.get('REPORT_DATE')
        if isinstance(report_date, str):
            try:
                report_date = dateutil.parser.parse(report_date)
            except ValueError:
                report_date = None

        return not (report_date and report_date >= datetime.datetime(2018, 12, 1, 0, 0, 0, 0))

    def _get_sort_order(self, match_document: Dict) -> list:
        """
        Populate the sort_order field on generated match documents.
        """
        sort_map = self._TRIAL_MATCH_SORTING
        sort_array = list()

        for sort_dimension in sort_map:
            sort_index = {99}
            for sort_key, sorting_values in sort_dimension.items():
                sort_val = sorting_values.get(match_document.get(sort_key))
                if sort_val is not None:
                    sort_index.add(sort_val)
            sort_array.append(min(sort_index))

        return sort_array

    _MMR_STATUS_DISPLAY_MAP = {
        'Proficient (MMR-P / MSS)': 'MMR-P/MSS',
        'Deficient (MMR-D / MSI-H)': 'MMR-D/MSI-H',
    }
    _CNV_CALL_DISPLAY_MAP = {
        "High Amplification": "High level amplification",
        "Homozygous Deletion": "Homozygous deletion",
        'Low Amplification': 'Gain',
        'Heterozygous Deletion': 'Heterozygous deletion',
    }
    _SIGNATURE_MAP = {
        'uva_signature': ('UVA_STATUS', 'UVA Signature'),
        'pole_signature': ('POLE_STATUS', 'POLE Signature'),
        'apobec_signature': ('APOBEC_STATUS', 'APOBEC Signature'),
        'temozolomide_signature': ('TEMOZOLOMIDE_STATUS', 'Temozolomide Signature'),
        'tobacco_signature': ('TABACCO_STATUS', 'Tobacco Signature'),
    }
    _TRIAL_MATCH_SORTING = [
        {"show_in_ui": {True: 1, False: -1}, "trial_curation_level_status": {"closed": -1, "open": 1}},
        {
            "match_type": {"mmr": 20, "tmb": 30},
            "temozolomide_status": {"Yes": 40},
            "apobec_status": {"Yes": 40},
            "tabacco_status": {"Yes": 40},
            "pole_status": {"Yes": 40},
            "uva_status": {"Yes": 40},
            "tier": {1: 50, 2: 60, 3: 80, 4: 90},
            "cnv_call": {
                "Homozygous deletion": 70,
                "High level amplification": 71,
                "Gain": 72,
                "Heterozygous deletion": 73,
            },
            "wildtype": {False: 100, True: 100},
        },
        {"match_type": {"variant": 0, "gene": 1}},
        {"variant_category": {"SV": 0}},
        {"cancer_type_match": {"_SOLID_": 100, "_LIQUID_": 100}},
        {"coordinating_center": {"Dana-Farber Cancer Institute": 0}},
    ]
    _INTERNAL_ID_MAPPING = {
        'step': 'step_internal_id',
        'arm': 'arm_internal_id',
        'dose_level': 'level_internal_id',
    }
    _CODE_MAPPING = {
        'step': 'step_code',
        'arm': 'arm_code',
        'dose_level': 'level_code',
    }
    _LEVEL_MAPPING = {
        'step': 'step',
        'arm': 'arm',
        'dose_level': 'dose',
    }
    _GENOMIC_COPY_FIELDS = {
        "TRUE_CDNA_CHANGE",
        "REFERENCE_ALLELE",
        "ALLELE_FRACTION",
        "STRUCTURAL_VARIANT_COMMENT",
        "SAMPLE_ID",
        "CLINICAL_ID",
        "VARIANT_CATEGORY",
        "WILDTYPE",
        "TRUE_TRANSCRIPT_EXON",
        "TIER",
        "TRUE_HUGO_SYMBOL",
        "TRUE_PROTEIN_CHANGE",
        "CNV_CALL",
        "TRUE_VARIANT_CLASSIFICATION",
        "MMR_STATUS",
        "STRUCTURAL_VARIANT_COMMENT",
        "CHROMOSOME",
        "ACTIONABILITY",
        "POSITION",
        "APOBEC_STATUS",
        "POLE_STATUS",
        "TABACCO_STATUS",
        "TEMOZOLOMIDE_STATUS",
        "UVA_STATUS",
        "LEFT_PARTNER_GENE",
        "RIGHT_PARTNER_GENE",
        "STRUCTURAL_VARIANT_TYPE",
        "MOLECULAR_FUNCTION",
        "MUTATION_EFFECT",
        "MS_STATUS"
    }
    _CLINICAL_COPY_FIELDS = {
        "REPORT_DATE",
        "GENDER",
        "SAMPLE_ID",
        "MRN",
        "ONCOTREE_PRIMARY_DIAGNOSIS_NAME",
        "TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE",
        "PATIENT_ID",
        "STUDY_ID",
        "VITAL_STATUS",
        "AGE",
        "HER2_STATUS",
        "PR_STATUS",
        "ER_STATUS"
    }
    _TRIAL_COPY_FIELDS = {'protocol_no', 'short_title', 'nickname', 'nct_id'}
    _PRIOR_TREATMENT_COPY_FIELDS = {
        'AGENT',
        'TREATMENT_CATEGORY',
        'SUBTYPE',
        'AGENT_CLASS',
        'SURGERY_TYPE',
        'RADIATION_TYPE',
        'RADIATION_SITE'
        }
