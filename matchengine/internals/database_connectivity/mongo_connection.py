from __future__ import annotations

from typing import TYPE_CHECKING
import motor.motor_asyncio
import pymongo.database

from matchengine.plugin_stub import DBSecrets

if TYPE_CHECKING:
    from typing import (
        Union,
    )

_MAX_TIMEOUT = 10 * 60 * 1000



class MongoDBConnection(object):
    uri = ""
    read_only: bool
    secrets: DBSecrets
    db: Union[pymongo.database.Database, motor.motor_asyncio.AsyncIOMotorDatabase]
    _client: Union[pymongo.MongoClient, motor.motor_asyncio.AsyncIOMotorClient]

    def __init__(self, read_only=True, db=None, async_init=True, loop=None, secrets=None):
        """
        Default params to use values from an external SECRETS.JSON configuration file,

        Override SECRETS_JSON values if arguments are passed via CLI
        :param read_only:
        :param db:
        """
        self.read_only = read_only
        self.async_init = async_init
        self.db = db
        self.secrets = (secrets or DBSecrets())
        self._loop = loop

    def __enter__(self):

        opts = self.secrets.get_mongodb_options(self.read_only)

        opts.update(dict(
            # We need *some* timeout to prevent indefinite hangs
            connectTimeoutMS=_MAX_TIMEOUT,
            socketTimeoutMS=_MAX_TIMEOUT,
            serverSelectionTimeoutMS=_MAX_TIMEOUT,
            connect=True,
        ))

        if self.async_init:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(**opts, io_loop=self._loop)
        else:
            self._client = pymongo.MongoClient(**opts)
        return self._client.get_database(self.db or self.secrets.get_database_name())

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self._client.close()
