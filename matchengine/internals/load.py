import csv
from datetime import datetime
import json
import logging
import os
from argparse import Namespace
from contextlib import ExitStack
from typing import List

import yaml
import io
from bson import json_util
from pathlib import Path

import json
from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection

log = logging.getLogger('matchengine.load')


def load(args: Namespace):
    """
    Load data into MongoDB for matching.

    Depending on the naming conventions used in your data, it may be necessary to either alter the data itself,
    or to define custom transformation functions in order for matching to work correctly.

    These transformations to data to prepare for matching can be made using the config.json file,
    and custom functions as described in match_criteria_transform.json.

    For more information and examples, see the README.
    """

    with ExitStack() as stack:
        db_rw = stack.enter_context(MongoDBConnection(read_only=False, db=args.db_name, async_init=False))
        log.info(f"Database: {args.db_name}")
        if args.trial:
            if hasattr(args, 'from_api'):
                if args.from_api:
                    load_trials_from_api(db_rw, args.trial)
                else:
                    load_trials(db_rw, args)
            else:
                load_trials(db_rw, args)

        if args.clinical:
            log.info('Adding clinical data to mongo...')
            load_clinical(db_rw, args)

        if args.genomic:
            if len(list(db_rw.clinical.find({}))) == 0:
                log.warning("No clinical documents in db. Please load clinical documents before loading genomic.")

            log.info('Adding genomic data to mongo...')
            load_genomic(db_rw, args)

        log.info('Done.')


#################
# trial loading
#################


def load_trials(db_rw, args: Namespace):
    trials = items_from_path(Path(args.trial))
    # for trial in trials:
    #     if 'protocol_no' not in trial:
    #         log.warning("Refusing to add trial without protocol_no")
    #         continue
    #     trial_del = db_rw['trial'].delete_many({'protocol_no': trial['protocol_no']})
    #     log.info(f"Loading trial with protocol_no: {trial.get('protocol_no')}")
    #     if trial_del.deleted_count:
    #         log.warning("Deleted existing duplicate trial")
    #
    #     db_rw.trial.insert_one(trial)
    load_trials_from_api(db_rw, trials)

def load_trials_from_api(db_rw, json_list: List[dict]):
    for trial in json_list:
        if 'protocol_no' not in trial:
            log.warning("Refusing to add trial without protocol_no")
            continue
        trial_del = db_rw['trial'].delete_many({'protocol_no': trial['protocol_no']})
        log.info(f"Loading trial with protocol_no: {trial.get('protocol_no')}")
        if trial_del.deleted_count:
            log.warning("Deleted existing duplicate trial")

        # ensure protocol_no is a string
        if isinstance(trial['protocol_no'], int):
            trial['protocol_no'] = str(trial['protocol_no'])
        db_rw.trial.insert_one(trial)


def items_from_path(root_path):
    source_paths = []
    if not root_path.exists():
        raise ValueError(f"path does not exist: {root_path}")
    if root_path.is_dir():
        source_paths = [
            path for glob in ['**/*.yml', '**/*.yaml', '**/*.json', '**/*.csv'] for path in root_path.glob(glob)
        ]
    else:
        source_paths = [root_path]

    return [item for path in source_paths for item in parse_data(path)]


def parse_data(path):
    contents = path.read_text()
    if path.suffix in {'.yml', '.yaml'}:
        for doc in yaml.safe_load_all(contents):
            if isinstance(doc, list):
                for subdoc in doc:
                    yield subdoc
            else:
                yield doc
    elif path.suffix in {'.ejson'}:  # MongoDB extended JSON
        doc = json_util.loads(contents)
        if isinstance(doc, list):
            for subdoc in doc:
                yield subdoc
        else:
            yield doc
    elif path.suffix in {'.json'}:
        while True:
            contents = contents.strip()
            if not contents:
                break
            decoder = json.JSONDecoder()
            doc, idx = decoder.raw_decode(contents)
            contents = contents[idx:]
            if isinstance(doc, list):
                for subdoc in doc:
                    yield subdoc
            else:
                yield doc
    elif path.suffix in {'.csv'}:
        reader = csv.DictReader(io.StringIO(contents), strict=True)
        for row in reader:
            yield row
    else:
        raise ValueError("invalid path")


def load_clinical(db_rw, args):
    if args.drop:
        r = db_rw['clinical'].delete_many({})
        log.info(f"Dropped {r.deleted_count} clinical documents")
    clinicals = items_from_path(Path(args.clinical))

    for c in clinicals:
        if isinstance(c.get('BIRTH_DATE'), str):
            fixed_date = datetime.strptime(c['BIRTH_DATE'], "%Y-%m-%d")
            c['BIRTH_DATE'] = fixed_date
            c['BIRTH_DATE_INT'] = int(fixed_date.strftime('%Y%m%d'))

    log.info(f"Loading {len(clinicals)} clinical documents")

    for c in clinicals:
        if 'SAMPLE_ID' not in c:
            log.warning("Refusing to add clinical document without SAMPLE_ID")
            continue

        clin_del = db_rw['clinical'].delete_many({'SAMPLE_ID': c['SAMPLE_ID']})
        gen_del = db_rw['genomic'].delete_many({'SAMPLE_ID': c['SAMPLE_ID']})
        if clin_del.deleted_count or gen_del.deleted_count:
            log.info(
                f"Removed {clin_del.deleted_count} duplicate clinical documents and {gen_del.deleted_count} genomic documents"
            )
        db_rw['clinical'].insert_one(c)


def load_genomic(db_rw, args):
    if args.drop:
        r = db_rw['genomic'].delete_many({})
        log.info(f"Dropped {r.deleted_count} genomic documents")
    genomics = items_from_path(Path(args.genomic))
    clinical_dict = {item['SAMPLE_ID']: item['_id'] for item in db_rw['clinical'].find({}, {'_id': 1, 'SAMPLE_ID': 1})}
    ok_genomics, bad_genomics = [], []
    for g in genomics:
        if g.get('CLINICAL_ID'):
            ok_genomics.append(g)
        elif g.get('SAMPLE_ID') in clinical_dict:
            g['CLINICAL_ID'] = clinical_dict[g['SAMPLE_ID']]
            ok_genomics.append(g)
        else:
            bad_genomics.append(g)

    if bad_genomics:
        log.warning(f"Ignoring {len(bad_genomics)} genomic documents with no corresponding clinical documents")

    log.info(f"Loading {len(ok_genomics)} genomic documents")
    for c in ok_genomics:
        db_rw['genomic'].insert_one(c)
