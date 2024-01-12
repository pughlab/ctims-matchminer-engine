# This file produces a mapping between oncotree types and all their subtypes from the MSK API
# The version currently used is oncotree_legacy_1.1

import requests
import json
from collections import defaultdict, namedtuple

TumorType = namedtuple('TumorType', ['code',  'name', 'parent', 'virtual' ])

def get_tumor_types():
    rj = requests.get('http://oncotree.mskcc.org/api/tumorTypes?version=oncotree_legacy_1.1').json()
    tumor_types = set( TumorType(code=item['code'], name=item['name'], parent=item['parent'], virtual=False) for item in rj )
    return tumor_types

def process_types(tumor_types):
    fixed_types = set([
        TumorType(code='_SOLID_', name='_SOLID_', parent=None, virtual=True),
        TumorType(code='_LIQUID_', name='_LIQUID_', parent=None, virtual=True)
        ])
    for tt in tumor_types:
        if tt.code == "TISSUE":
            continue

        if tt.parent == "TISSUE":
            if tt.code in ("LYMPH", "BLOOD"):
                tt = tt._replace(parent='_LIQUID_')
            else:
                tt = tt._replace(parent='_SOLID_')

        # EDW leaves out the apostrophes
        tt = tt._replace(name=tt.name.replace("'", ""))

        fixed_types.add(tt)
    fixed_types.update([
        TumorType(code='_NOTFOUND_', name='No OncoTree Node Found', parent='_SOLID_', virtual=False)
        ])
    return fixed_types

tumor_types = process_types(get_tumor_types())
parent_code_to_tumor_types = defaultdict(set)
for tt in tumor_types:
    parent_code_to_tumor_types[tt.parent].add(tt)

descendants_memo = {}
def get_descendants(tumor_type):
    if tumor_type.code in descendants_memo:
        return descendants_memo[tumor_type.code]
    result = set([tumor_type])
    for child in parent_code_to_tumor_types[tumor_type.code]:
        result.update(get_descendants(child))
    descendants_memo[tumor_type.code] = result
    return result

name_map = defaultdict(set)
for tt in tumor_types:
    name_map[tt.name].update(set(x.name for x in get_descendants(tt) if not x.virtual))

results = { k : sorted(v) for k, v in name_map.items() }

with open('oncotree_mapping.json', 'w') as f:
    json.dump(results, f, sort_keys=True, indent=2, ensure_ascii=False)
