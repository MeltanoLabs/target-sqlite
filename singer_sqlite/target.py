from singer_sdk import SQLTarget

from singer_sqlite.config import target_config
from singer_sqlite.sink import SQLiteSink

DB_PATH_CONFIG = "path_to_db"


class SQLiteTarget(SQLTarget):
    """The Tap class for SQLite."""

    name = "target-sqlite-sample"
    default_sink_class = SQLiteSink
    max_parallelism = 1

    config_jsonschema = target_config.to_dict()
