from typing import Any, Dict

import sqlalchemy

from singer_sdk import SQLConnector

DB_PATH_CONFIG = "path_to_db"


class SQLiteConnector(SQLConnector):
    """The connector for SQLite.

    This class handles all DDL and type conversions.
    """

    allow_temp_tables = False
    allow_column_alter = False
    allow_merge_upsert = True

    def get_sqlalchemy_url(self, config: Dict[str, Any]) -> str:
        """Generates a SQLAlchemy URL for SQLite."""
        return f"sqlite:///{config[DB_PATH_CONFIG]}"

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        This override simply provides a more helpful error message on failure.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        try:
            return super().create_sqlalchemy_connection()
        except Exception as ex:
            raise RuntimeError(
                f"Error connecting to DB at '{self.config[DB_PATH_CONFIG]}'"
            ) from ex
