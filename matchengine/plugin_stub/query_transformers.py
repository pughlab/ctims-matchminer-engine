
import datetime
from dateutil.relativedelta import relativedelta
from matchengine.internals.typing.matchengine_types import QueryTransformerResult


class QueryTransformers(object):
    """
    QueryTransformers stores a collection of methods corresponding to the sample_key_mappings in
    the configuration file; each one translates a CTML key/value pair into parts of a MongoDB query.
    """
    def _is_negate(self, trial_value):
        """
        Example: !EGFR => (True, EGFR)

        :param trial_value:
        :return:
        """
        negate = True if trial_value.__class__ is str and trial_value and trial_value[0] == '!' else False
        trial_value = trial_value[1::] if negate else trial_value
        return trial_value, negate

    def nomap(self, sample_key, trial_value, **kwargs):
        trial_value, negate = self._is_negate(trial_value)
        return QueryTransformerResult({sample_key: trial_value}, negate)

    def age_range_to_date_query(self, sample_key, trial_value, current_date, **kwargs):
        operator_map = {
            "==": "$eq",
            "<=": "$gte",
            ">=": "$lte",
            ">": "$lte",
            "<": "$gte",
        }
        # funky logic is because 1 month curation is curated as "0.083" (1/12 a year)
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        if numeric.startswith('.'):
            numeric = '0' + numeric
        split_time = numeric.split('.')
        years = int(split_time[0] if split_time[0].isdigit() else 0)
        months_fraction = float('0.' + split_time[1]) if len(split_time) > 1 else 0
        months = round(months_fraction * 12)
        query_date = current_date + (-relativedelta(years=years, months=months))
        query_datetime = datetime.datetime(query_date.year, query_date.month, query_date.day, query_date.hour, 0, 0, 0)
        return QueryTransformerResult({sample_key: {operator_map[operator]: query_datetime}}, False)

    def age_range_to_date_int_query(self, sample_key, trial_value, current_date, **kwargs):
        operator_map = {
            "==": "$eq",
            "<=": "$gte",
            ">=": "$lte",
            ">": "$lte",
            "<": "$gte",
        }
        # funky logic is because 1 month curation is curated as "0.083" (1/12 a year)
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        if numeric.startswith('.'):
            numeric = '0' + numeric
        split_time = numeric.split('.')
        years = int(split_time[0] if split_time[0].isdigit() else 0)
        months_fraction = float('0.' + split_time[1]) if len(split_time) > 1 else 0
        months = round(months_fraction * 12)
        query_date = current_date + (-relativedelta(years=years, months=months))
        return QueryTransformerResult({sample_key: {operator_map[operator]: int(query_date.strftime('%Y%m%d'))}}, False)
