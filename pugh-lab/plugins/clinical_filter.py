from __future__ import annotations

import datetime
import dateutil.parser

from matchengine.plugin_stub import ClinicalFilter
from matchengine.internals.typing.matchengine_types import QueryNode


class PughLabClinicalFilter(ClinicalFilter):
    def should_match(self, doc: dict):
        return True

    def should_filter(self, query_node: QueryNode):
        # DFCI provided structural variant data in a structured format only starting Dec. 1st 2018
        # Patients with reports from before this date should not have structural variants shown in UI
        has_structured_sv = query_node.get_query_part_by_key('STRUCTURED_SV') is not None
        has_unstructured_sv = query_node.get_query_part_by_key('STRUCTURAL_VARIANT_COMMENT') is not None

        if has_structured_sv or has_unstructured_sv:
            return True
        else:
            return False

    def apply_filter(self, query_node: QueryNode, doc: dict) -> bool:

        has_structured_sv = query_node.get_query_part_by_key('STRUCTURED_SV') is not None
        has_unstructured_sv = query_node.get_query_part_by_key('STRUCTURAL_VARIANT_COMMENT') is not None

        report_date = doc.get('REPORT_DATE')
        if isinstance(report_date, str):
            try:
                report_date = dateutil.parser.parse(report_date)
            except ValueError:
                report_date = None

        is_after_structured_sv_available = report_date and report_date >= datetime.datetime(2018, 12, 1, 0, 0, 0, 0)

        if has_structured_sv:
            return is_after_structured_sv_available
        elif has_unstructured_sv:
            return not is_after_structured_sv_available
        else:
            return True
