from singer_sdk import SQLTap

from singer_sqlite.config import tap_config
from singer_sqlite.stream import SQLiteStream


class SQLiteTap(SQLTap):
    """The Tap class for SQLite."""

    name = "target-sqlite-sample"
    default_stream_class = SQLiteStream
    max_parallelism = 1

    config_jsonschema = tap_config.to_dict()
