"""A sample implementation for SQLite."""

from singer_sdk import SQLSink

from singer_sqlite.connector import SQLiteConnector


class SQLiteSink(SQLSink):
    """The Sink class for SQLite.

    This class allows developers to optionally override `get_records()` and other
    stream methods in order to improve performance beyond the default SQLAlchemy-based
    interface.

    DDL and type conversion operations are delegated to the connector logic specified
    in `connector_class` or by overriding the `connector` object.
    """

    connector_class = SQLiteConnector
