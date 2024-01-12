from __future__ import annotations

from matchengine.plugin_stub import QueryProcessor
from matchengine.internals.typing.matchengine_types import QueryNode, QueryNodeContainer, QueryPart
import re
from typing import Union, List, Tuple, Dict, NoReturn


class CustomQueryProcessor(QueryProcessor):
    def transform(self, query_container: QueryNodeContainer):
        for query_node in query_container.query_nodes:
            self._query_node_transform(query_node)

    def _query_node_transform(self, query_node: QueryNode) -> NoReturn:
        """
        If a trial curation key/value requires alteration to a separate AND clause in the mongo query, do that here.
        Used to modify a query part dependent on another query part
        :return:
        """

        # If a trial curation calls for a structural variant but does NOT have the structured SV data field
        # FUSION_PARTNER_HUGO_SYMBOL, then the extended_attributes query is done using a regex search of the free text
        # STRUCTURAL_VARIANT_COMMENT field on the patient's extended_attributes document.
        whole_query = query_node.extract_raw_query()
        # encode as full search criteria
        if 'STRUCTURAL_VARIANT_COMMENT' in whole_query:
            for do_not_render_part_name in ['HARMONIZED_HUGO_SYMBOL', 'FUSION_PARTNER_HUGO_SYMBOL']:
                do_not_render_part = query_node.get_query_part_by_key(do_not_render_part_name)
                if do_not_render_part is not None:
                    do_not_render_part.render = False
            gene = whole_query.get('HARMONIZED_HUGO_SYMBOL')
            sv_part = query_node.get_query_part_by_key('STRUCTURAL_VARIANT_COMMENT')
            if 'STRUCTURED_SV' in whole_query:
                sv_part.render = False
            else:
                sv_part.set_query_attr(
                    'STRUCTURAL_VARIANT_COMMENT',
                    re.compile(rf"(.*\W{gene}\W.*)|(^{gene}\W.*)|(.*\W{gene}$)", re.IGNORECASE),
                )
        # blank-GENE -> Intergenic
        # GENE-blank -> Intergenic
        # GENE1-GENE1 -> GENE1-GENE1 # Intragenic
        # GENE1-GENE2 -> GENE1-GENE2
        elif 'STRUCTURED_SV' in whole_query:
            sv_info_part = query_node.get_query_part_by_key('STRUCTURED_SV')
            sv_info_part.render = False
            left = query_node.get_query_part_value_by_key('HARMONIZED_HUGO_SYMBOL', None)
            right = query_node.get_query_part_value_by_key('FUSION_PARTNER_HUGO_SYMBOL', None)
            for do_not_render_part_name in ['HARMONIZED_HUGO_SYMBOL', 'FUSION_PARTNER_HUGO_SYMBOL']:
                do_not_render_part = query_node.get_query_part_by_key(do_not_render_part_name)
                if do_not_render_part is not None:
                    do_not_render_part.render = False
            left_query = self._build_structured_sv_query(left, right, 'LEFT-RIGHT')
            right_query = self._build_structured_sv_query(left, right, 'RIGHT-LEFT')
            new_query = (
                {'$or': [left_query, right_query]}
                if left_query != right_query
                else left_query
            )
            query_node.add_query_part(QueryPart(new_query, sv_info_part.negate, True))

        # if signature curation is passed, do not query HARMONIZED_HUGO_SYMBOL
        if {
            'UVA_STATUS',
            'TOBACCO_STATUS',
            'POLE_STATUS',
            'TEMOZOLOMIDE_STATUS',
            'MMR_STATUS',
            'APOBEC_STATUS',
        }.intersection(set(whole_query.keys())):
            gene_part = query_node.get_query_part_by_key('HARMONIZED_HUGO_SYMBOL')
            if gene_part is not None:
                gene_part.render = False

    def _get_sv_query_value_and_field_name(
        self,
        left_side: Union[str, None],
        right_side: Union[str, None],
        sv_query_type: str,
    ) -> List[Tuple]:
        sides = [left_side, right_side]
        forward_fields = ['HARMONIZED_LEFT_PARTNER_GENE', 'HARMONIZED_RIGHT_PARTNER_GENE']
        backward_fields = forward_fields[::-1]
        fields = (
            forward_fields if sv_query_type == 'LEFT-RIGHT' else backward_fields
        )  # backward_fields if sv_query_type == 'RIGHT-LEFT
        return [(side, field) for side, field in zip(sides, fields)]

    def _build_structured_sv_query(
        self,
        left,
        right,
        sv_query_type,
    ) -> Dict:
        whole_query = dict()
        for side, field_name in self._get_sv_query_value_and_field_name(left, right, sv_query_type):
            query = dict()
            if side is None:
                pass
            elif side.lower() == 'intergenic':
                query = {field_name: None}
            elif side.lower().replace(' ', '_') == 'any_gene':
                query = {field_name: {'$ne': None}}
            else:
                query = {field_name: side}
            if query:
                whole_query.update(query)
        return whole_query
