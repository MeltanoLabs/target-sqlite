import pytest
import os

from jsonschema import ValidationError
from sqlalchemy import create_engine, inspect, text

from target_sqlite.target_sqlite import TargetSQLite


def load_stream(filename):
    myDir = os.path.dirname(os.path.abspath(__file__))
    stream = os.path.join(myDir, "data_files", filename)
    with open(stream) as f:
        return [line for line in f]


@pytest.fixture(scope="class")
def sqlite_engine(config):
    db_file = f'{config["database"]}.db'
    try:
        yield create_engine(f"sqlite:///{db_file}", future=True)
    finally:
        os.remove(db_file)


class TestTargetSQLite:
    def test_record_before_schema(self, config):
        test_stream = "record_before_schema.stream"
        target = TargetSQLite(config)
        stream = load_stream(test_stream)

        with pytest.raises(Exception) as excinfo:
            for line in stream:
                target.process_line(line)
        assert "encountered before a corresponding schema" in str(excinfo.value)

    def test_invalid_schema(self, config):
        test_stream = "invalid_schema.stream"
        target = TargetSQLite(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            for line in stream:
                target.process_line(line)
        assert "Not supported schema" in str(excinfo.value)

    def test_record_missing_key_property(self, config, sqlite_engine):
        test_stream = "record_missing_key_property.stream"
        target = TargetSQLite(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            for line in stream:
                target.process_line(line)
        assert "id" in str(excinfo.value)

        # Drop the Test Tables
        for stream, loader in target.loaders.items():
            loader.table.drop(loader.engine)

    def test_record_missing_required_property(self, config, sqlite_engine):
        test_stream = "record_missing_required_property.stream"
        target = TargetSQLite(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            for line in stream:
                target.process_line(line)
        assert "'id' is a required property" in str(excinfo.value)

        # Drop the Test Tables
        for stream, loader in target.loaders.items():
            loader.table.drop(loader.engine)

    @pytest.mark.slow
    def test_camelcase(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": ["test_camelcase"],
            "columns": {
                "test_camelcase": ["id", "client_name", config["timestamp_column"]]
            },
            "total_records": {"test_camelcase": 2},
        }

        test_stream = "camelcase.stream"

        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=False
        )

        # We also need to test that the record has data in the camelcased field
        with sqlite_engine.begin() as connection:
            item_query = text("SELECT client_name FROM test_camelcase")
            item_result = connection.execute(item_query).fetchone()
            assert item_result[0] == "Gitter Windows Desktop App"

    @pytest.mark.slow
    def test_special_chars_in_attributes(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": ["test_special_chars_in_attributes"],
            "columns": {
                "test_special_chars_in_attributes": [
                    "_id",
                    "d__env",
                    "d__agent_type",
                    "d__agent_os_version",
                    config["timestamp_column"],
                ]
            },
            "total_records": {"test_special_chars_in_attributes": 1},
        }

        test_stream = "special_chars_in_attributes.stream"

        self.integration_test(config, sqlite_engine, expected_results, test_stream)

    @pytest.mark.slow
    def test_optional_attributes(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {"test_optional_attributes": 4},
            "tables": ["test_optional_attributes"],
            "columns": {
                "test_optional_attributes": [
                    "id",
                    "optional",
                    config["timestamp_column"],
                ]
            },
            "total_records": {"test_optional_attributes": 4},
        }

        test_stream = "optional_attributes.stream"

        self.integration_test(config, sqlite_engine, expected_results, test_stream)

    @pytest.mark.slow
    def test_schema_no_properties(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": [
                "test_object_schema_with_properties",
                "test_object_schema_no_properties",
            ],
            "columns": {
                "test_object_schema_with_properties": [
                    "object_store__id",
                    "object_store__metric",
                    config["timestamp_column"],
                ],
                "test_object_schema_no_properties": [
                    "object_store",
                    config["timestamp_column"],
                ],
            },
            "total_records": {
                "test_object_schema_with_properties": 2,
                "test_object_schema_no_properties": 2,
            },
        }

        test_stream = "schema_no_properties.stream"

        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=False
        )

        # We also need to test that the proper data records were stored
        with sqlite_engine.connect() as connection:
            query = text(
                "SELECT COUNT(*) "
                " FROM test_object_schema_with_properties "
                " WHERE object_store__id = 1 AND object_store__metric = 187"
            )
            result = connection.execute(query).fetchone()
            assert result[0] == 1

            query = text("""
                SELECT COUNT(*)
                FROM test_object_schema_no_properties
                WHERE object_store = '{"id": 1, "metric": 1}'
            """)
            result = connection.execute(query).fetchone()
            assert result[0] == 1

        # Drop the Test Tables
        with sqlite_engine.begin() as connection:
            connection.execute(text("DROP TABLE test_object_schema_with_properties"))
            connection.execute(text("DROP TABLE test_object_schema_no_properties"))

    @pytest.mark.slow
    def test_schema_updates(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {"test_schema_updates": 6},
            "tables": ["test_schema_updates"],
            "columns": {
                "test_schema_updates": [
                    "id",
                    "a1",
                    "a2",
                    "a3",
                    "a4__id",
                    "a4__value",
                    "a5",
                    "a6",
                    config["timestamp_column"],
                ]
            },
            "total_records": {"test_schema_updates": 6},
        }

        test_stream = "schema_updates.stream"

        self.integration_test(config, sqlite_engine, expected_results, test_stream)

    @pytest.mark.slow
    def test_multiple_state_messages(self, capsys, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {
                "test_multiple_state_messages_a": 5,
                "test_multiple_state_messages_b": 6,
            },
            "tables": [
                "test_multiple_state_messages_a",
                "test_multiple_state_messages_b",
            ],
            "columns": {
                "test_multiple_state_messages_a": [
                    "id",
                    "metric",
                    config["timestamp_column"],
                ],
                "test_multiple_state_messages_b": [
                    "id",
                    "metric",
                    config["timestamp_column"],
                ],
            },
            "total_records": {
                "test_multiple_state_messages_a": 6,
                "test_multiple_state_messages_b": 6,
            },
        }

        test_stream = "multiple_state_messages.stream"

        updated_config = {**config, "batch_size": 3}
        self.integration_test(
            updated_config, sqlite_engine, expected_results, test_stream
        )

        # Check that the expected State messages where flushed
        expected_stdout = [
            '{"test_multiple_state_messages_a": 1, "test_multiple_state_messages_b": 0}',
            '{"test_multiple_state_messages_a": 3, "test_multiple_state_messages_b": 2}',
            '{"test_multiple_state_messages_a": 5, "test_multiple_state_messages_b": 6}',
            "",
        ]

        captured = capsys.readouterr()
        assert captured.out == "\n".join(expected_stdout)

    @pytest.mark.slow
    def test_relational_data(self, config, sqlite_engine):
        # Start with a simple initial insert for everything

        # The expected results to compare
        expected_results = {
            "state": {"test_users": 5, "test_locations": 3, "test_user_in_location": 3},
            "tables": ["test_users", "test_locations", "test_user_in_location"],
            "columns": {
                "test_users": ["id", "name", config["timestamp_column"]],
                "test_locations": ["id", "name", config["timestamp_column"]],
                "test_user_in_location": [
                    "id",
                    "user_id",
                    "location_id",
                    "info__weather",
                    "info__mood",
                    config["timestamp_column"],
                ],
            },
            "total_records": {
                "test_users": 5,
                "test_locations": 3,
                "test_user_in_location": 3,
            },
        }

        test_stream = "user_location_data.stream"

        # We are not dropping the schema after the first integration test
        #  in order to also test UPSERTing records to SQLite
        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=False
        )

        # And then test Upserting (Combination of Updating already available
        #   rows and inserting a couple new rows)
        expected_results["state"] = {
            "test_users": 13,
            "test_locations": 8,
            "test_user_in_location": 14,
        }

        expected_results["total_records"] = {
            "test_users": 8,
            "test_locations": 5,
            "test_user_in_location": 5,
        }
        test_stream = "user_location_upsert_data.stream"

        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=True
        )

    @pytest.mark.slow
    def test_no_primary_keys(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {"test_no_pk": 3},
            "tables": ["test_no_pk"],
            "columns": {"test_no_pk": ["id", "metric", config["timestamp_column"]]},
            "total_records": {"test_no_pk": 3},
        }

        test_stream = "no_primary_keys.stream"

        # We are not dropping the schema after the first integration test
        #  in order to also test APPENDING records when no PK is defined
        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=False
        )

        # And then test Upserting
        # The Total Records in this case should be 8 (5+3) due to the records
        #  being appended and not UPSERTed
        expected_results["state"] = {"test_no_pk": 5}
        expected_results["total_records"] = {"test_no_pk": 8}

        test_stream = "no_primary_keys_append.stream"

        self.integration_test(
            config, sqlite_engine, expected_results, test_stream, drop_schema=True
        )

    def test_duplicate_records(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {"test_duplicate_records": 2},
            "tables": ["test_duplicate_records"],
            "columns": {
                "test_duplicate_records": ["id", "metric", config["timestamp_column"]]
            },
            "total_records": {"test_duplicate_records": 2},
        }

        test_stream = "duplicate_records.stream"

        self.integration_test(config, sqlite_engine, expected_results, test_stream)

    @pytest.mark.slow
    def test_array_data(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {"test_carts": 4},
            "tables": ["test_carts"],
            "columns": {"test_carts": ["id", "fruits", config["timestamp_column"]]},
            "total_records": {"test_carts": 4},
        }

        test_stream = "array_data.stream"

        self.integration_test(
            config,
            sqlite_engine,
            expected_results,
            test_stream,
            drop_schema=False,
        )

        # We also need to test that the proper data records were stored
        with sqlite_engine.connect() as connection:
            query = text("""
                SELECT json_array_length(fruits) AS size
                FROM test_carts
                ORDER BY id
            """)
            result = connection.execute(query).fetchall()
            assert result[0][0] == 3
            assert result[1][0] == 2
            assert result[2][0] == 1
            assert result[3][0] == 4

        # Drop the Test Table
        with sqlite_engine.begin() as connection:
            connection.execute(text("DROP TABLE test_carts"))

    @pytest.mark.slow
    def test_encoded_string_data(self, config, sqlite_engine):
        # The expected results to compare
        expected_results = {
            "state": {
                "test_strings": 11,
                "test_strings_in_objects": 11,
                "test_strings_in_arrays": 6,
            },
            "tables": [
                "test_strings",
                "test_strings_in_objects",
                "test_strings_in_arrays",
            ],
            "columns": {
                "test_strings": ["id", "info", config["timestamp_column"]],
                "test_strings_in_objects": [
                    "id",
                    "info__name",
                    "info__value",
                    config["timestamp_column"],
                ],
                "test_strings_in_arrays": ["id", "strings", config["timestamp_column"]],
            },
            "total_records": {
                "test_strings": 11,
                "test_strings_in_objects": 11,
                "test_strings_in_arrays": 6,
            },
        }

        test_stream = "encoded_strings.stream"

        self.integration_test(config, sqlite_engine, expected_results, test_stream)

    def integration_test(
        self, config, sqlite_engine, expected, stream_file, drop_schema=True
    ):
        try:
            # Create the TargetSQLite and fully run it using the user_location_data
            target = TargetSQLite(config)

            stream = load_stream(stream_file)

            for line in stream:
                target.process_line(line)

            target.flush_all_cached_records()

            # Check that the final state is the expected one
            assert target.last_emitted_state == expected["state"]

            # Check that the requested schema has been created
            inspector = inspect(sqlite_engine)

            all_table_names = inspector.get_table_names()

            with sqlite_engine.begin() as connection:
                for table in expected["tables"]:
                    # Check that the Table has been created in SQLite
                    assert table in all_table_names

                    # Check that the Table created has the requested attributes
                    db_columns = []
                    columns = inspector.get_columns(table)
                    for column in columns:
                        db_columns.append(column["name"].lower())
                        assert column["name"].lower() in expected["columns"][table]

                    for column in expected["columns"][table]:
                        assert column in db_columns

                    # Check that the correct number of rows were inserted
                    query = text(f"SELECT COUNT(*) FROM {table}")

                    results = connection.execute(query).fetchone()
                    assert results[0] == expected["total_records"][table]
        finally:
            # Drop the Test Tables
            if drop_schema:
                for stream, loader in target.loaders.items():
                    loader.table.drop(loader.engine)
