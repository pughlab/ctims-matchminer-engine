import os
import json

class SecretsParsingException(ValueError):
    pass

class DBSecrets:
    """
    The default implementation of the class for retrieving database connection parameters.
    """
    _secrets: dict

    def __init__(self):
        self._secrets = None

    def _load_json(self):
        if self._secrets:
            return
        secrets_json = os.getenv('SECRETS_JSON', None)
        if secrets_json is None:
            raise SecretsParsingException("SECRETS_JSON not set; exiting")
        try:
            if os.path.exists(secrets_json):
                with open(secrets_json) as _f:
                    self._secrets = json.load(_f)
            else:
                self._secrets = json.loads(secrets_json)
        except Exception as e:
            raise SecretsParsingException("SECRETS_JSON not valid json; exiting") from e


    def get_mongodb_options(self, read_only: bool):
        self._load_json()
        s = self._secrets
        try:
            options = dict(
                host=s["MONGO_HOST"],
                port=s["MONGO_PORT"],
                authSource=s.get("MONGO_AUTH_SOURCE"),
                replicaSet=s.get("MONGO_REPLICASET"),
            )
            if read_only:
                options.update(dict(
                    username=s["MONGO_RO_USERNAME"],
                    password=s["MONGO_RO_PASSWORD"],
                ))
            else:
                options.update(dict(
                    username=s["MONGO_USERNAME"],
                    password=s["MONGO_PASSWORD"],
                ))
        except KeyError as e:
            raise SecretsParsingException("SECRETS_JSON is missing key") from e

        return {k : v for k, v in options.items() if v not in [None, ""]}

    def get_database_name(self):
        self._load_json()
        return self._secrets["MONGO_DBNAME"]


