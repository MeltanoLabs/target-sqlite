import logging
from pathlib import Path

from typing import Dict, List
from sqlalchemy import create_engine, inspect, Table, text
from sqlalchemy.event import listen
from sqlalchemy import exc


# Map sqlalchemy types to SQLite Types
# Required for two reasons:
# 1. Compare the sqlalchemy Table definition to what is defined in SQLite
# 2. Use the type to manually execute an ALTER TABLE for updating or
#    adding new columns
MAP_SQLALCHEMY_TO_SQLITE_TYPE = {
    "BIGINT": "INTEGER",
    "FLOAT": "REAL",
    "VARCHAR": "TEXT",
    "BOOLEAN": "INTEGER",
    "TIMESTAMP": "TEXT",
}


class SQLiteLoader:
    def __init__(self, table: Table, config: Dict) -> None:
        self.table = table
        self.database_path = Path(config["database"]).with_suffix(".db")

        self.engine = create_engine(f"sqlite:///{self.database_path}", future=True)
        listen(self.engine, "first_connect", self.enable_wal)

    def enable_wal(cls, conn, conn_record):
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    def attribute_names(self) -> List[str]:
        """
        Get the attribute(column) names for the associated Table
        """
        return [column.name for column in self.table.columns]

    def empty_record(self) -> Dict:
        """
        Get a dictionary representing an empty (all attributes None) record for
        the table associated with this SQLiteLoader instance.

        Used as a template in order to normalize (map) all imported records to
        the full schema they are defined for.

        Important for records with multiple optional attributes that are not
        always there, like for example Multi Level JSON objects that are
        flattened before uploaded to SQLite.

        Guards against sqlalchemy errors for missing required values for
        bind parameters.
        """
        return dict.fromkeys(column.name for column in self.table.columns)

    def schema_apply(self) -> None:
        """
        Apply the schema defined for self.table to the Database we connect to
        """
        inspector = inspect(self.engine)

        all_table_names = inspector.get_table_names(self.table.schema)
        if self.table.name not in all_table_names:
            logging.debug(f"Table {self.table.name} does not exist -> creating it ")
            self.table.create(self.engine)
        else:
            # There is an existing Table: Check if a schema update is required
            self.schema_update(inspector)

    def schema_update(self, inspector) -> None:
        """
        Check if there is a schema diff between the new Table and the existing
        one and if the changes can be supported, update the table with the diff.

        Rules:
        1. Only support type upgrades (e.g. STRING -> VARCHAR) for existing columns
        2. If a not supported type update is requested (e.g. float --> int)
           raise a SchemaUpdateError exception.
        2. Never drop columns, only update or add new ones
        """
        existing_columns = {}
        columns_to_add = []

        # Fetch the existing defined tables and store them in a format useful
        #  for comparisors.
        all_columns = inspector.get_columns(self.table.name)

        for column in all_columns:
            existing_columns[column["name"]] = f"{column['type']}"

        # Check the new Table definition for new attributes or attributes
        #  with an updated data type
        for column in self.table.columns:
            # SQLITE does not support updating existing columns so only add new ones
            if column.name not in existing_columns:
                # A new column to be added to the table
                column_type = MAP_SQLALCHEMY_TO_SQLITE_TYPE[f"{column.type}"]
                columns_to_add.append((column.name, column_type))

        # If there are any columns to add, make the schema update
        for name, type in columns_to_add:
            self.add_column(name, type)

    def add_column(self, col_name: str, col_data_type: str) -> None:
        """
        Add the requested column to the SQLite Table defined by self.table
        """
        full_name = self.table.name
        alter_stmt = f"ALTER TABLE {full_name} ADD COLUMN {col_name} {col_data_type}"

        logging.debug(f"Adding COLUMN {col_name} ({col_data_type}) to {full_name}")

        with self.engine.begin() as connection:
            connection.execute(text(alter_stmt))

    def load(self, data: List[Dict]) -> None:
        """
        Load the data provided as a list of dictionaries to the given Table
        """
        if not data:
            return

        logging.debug(f"Loading data to SQLite for {self.table.name}")
        if self.table.primary_key:
            # We have to use SQLite's Upsert but the default SQLite for python
            #  does not yet support the "ON CONFLICT" clause for upserting
            # So, we'll follow the slow but stable approach of inserting each
            #  row and updating on conflict.
            with self.engine.begin() as connection:
                for row in data:
                    try:
                        connection.execute(self.table.insert(), row)
                    except exc.IntegrityError:
                        statement = self.table.update()  # .where()
                        for primary_key in self.table.primary_key:
                            statement = statement.where(
                                primary_key == row[primary_key.name]
                            )

                        connection.execute(statement, row)
        else:
            # Just Insert (append) as no conflicts can arise
            with self.engine.begin() as connection:
                connection.execute(self.table.insert(), data)
