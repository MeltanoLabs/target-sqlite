"""A sample implementation for SQLite."""

from singer_sdk import SQLStream

from singer_sqlite.connector import SQLiteConnector


class SQLiteStream(SQLStream):
    """The Stream class for SQLite.

    This class allows developers to optionally override `process_batch()` and other
    sink methods in order to improve performance beyond the default SQLAlchemy-based
    interface.

    DDL and type conversion operations are delegated to the connector logic specified
    in `connector_class` or by overriding the `connector` object.
    """

    connector_class = SQLiteConnector
