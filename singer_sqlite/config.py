from singer_sdk import typing as th

DB_PATH_CONFIG = "path_to_db"

_shared_config = th.PropertiesList(
    th.Property(
        DB_PATH_CONFIG,
        th.StringType,
        description="The path to your SQLite database file(s).",
    )
)
tap_config = _shared_config
target_config = _shared_config

__all__ = ["DB_PATH_CONFIG", "tap_config", "target_config"]
