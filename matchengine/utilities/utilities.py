from __future__ import annotations
import glob
import logging
import os
import sys
from types import MethodType
from typing import TYPE_CHECKING

from matchengine import query_transform
from matchengine.utilities.mongo_connection import MongoDBConnection
from matchengine.plugin_stub import (
    QueryTransformerContainer,
    TrialMatchDocumentCreator,
    DBSecrets,
    QueryNodeTransformer
)

if TYPE_CHECKING:
    from typing import Dict, List
    from matchengine.engine import MatchEngine
    from matchengine.utilities.matchengine_types import MongoQuery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def perform_db_call(matchengine: MatchEngine, collection: str, query: MongoQuery, projection: Dict) -> List:
    """
    Asynchronously executes a find query on the database, with specified query and projection and a collection
    Used to parallelize DB calls, with asyncio.gather
    """
    return await matchengine.async_db_ro[collection].find(query, projection).to_list(None)


def find_plugins(me):
    """
    Plugins are *.py files located in the ./plugins directory. They must be python classes which inherit either from
    QueryTransformerContainer or TrialMatchDocumentCreator.

    For more information on how the plugins function, see the README.
    """
    log.info(f"Checking for plugins in {me.plugin_dir}")
    potential_files = glob.glob(os.path.join(me.plugin_dir, "*.py"))
    to_load = [(None, 'matchengine.query_transform')]
    for potential_file_path in potential_files:
        dir_path = os.path.dirname(potential_file_path)
        module_name = ''.join(os.path.basename(potential_file_path).split('.')[0:-1])
        to_load.append((dir_path, module_name))
    for dir_path, module_name in to_load:
        if dir_path is not None:
            sys.path.append(dir_path)
        module = __import__(module_name)
        module_path = module_name.split('.')
        if len(module_path) > 1:
            for sub_item in module_path[1::]:
                module = getattr(module, sub_item)
        if dir_path is not None:
            sys.path.pop()
        for item_name in getattr(module, '__shared__', list()):
            log.info(f"Found shared plugin resource {item_name} in module {module_name}, path {dir_path}")
            setattr(me.match_criteria_transform.transform, item_name, getattr(module, item_name))
        for item_name in module.__export__:
            item = getattr(module, item_name)
            log.info(f"Found exported plugin item {item_name} in module {module_name}, path {dir_path}")
            if issubclass(item, QueryTransformerContainer):
                log.info(f"Loading QueryTransformerContainer {item_name} type: {item}")
                query_transform.attach_transformers_to_match_criteria_transform(me.match_criteria_transform,
                                                                                item)
            elif issubclass(item, TrialMatchDocumentCreator):
                if item_name == me.match_document_creator_class:
                    log.info(f"Loading TrialMatchDocumentCreator {item_name} type: {item}")
                    setattr(me,
                            'create_trial_matches',
                            MethodType(getattr(item,
                                               'create_trial_matches'),
                                       me))
            elif issubclass(item, DBSecrets):
                if item_name == me.db_secrets_class:
                    log.info(f"Loading DBSecrets {item_name} type: {item}")
                    secrets = item().get_secrets()
                    setattr(MongoDBConnection, 'secrets', secrets)
            elif issubclass(item, QueryNodeTransformer):
                if item_name == me.query_node_transformer_class:
                    log.info(f"Loading QueryNodeTransformer {item_name} type: {item}")
                    setattr(me,
                            "query_node_transform",
                            MethodType(getattr(item,
                                               "query_node_transform"),
                                       me))