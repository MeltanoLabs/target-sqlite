# target-sqlite

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
and loads them to SQLite.


## Installation

1. Create and activate a virtualenv
2. `pip install -e '.[dev]'`  

## Configuration of target-sqlite

**config.json**
```json
{
  "database": "The name of the SQLite DB to be used (i.e. the name of the file.db that will be created)",

  "batch_size": "How many records are loaded to SQLite at a time? Default=50",

  "timestamp_column": "Name of the column used for recording the timestamp when Data are loaded to SQLite. Default=__loaded_at"
}
```


## Simple test run

If you want to quickly test that your setup is properly configured, you can:

`pytest -vv tests/ --config config.json`


This includes a set of simple tests to check that the connection to SQLite is properly set and that all the required SQLite operations work as expected.

During the tests we create a test tables, populate them with simple data, assert that both the schema and the data loaded are as expected and in the end we destroy them.

In addition, we have also full integration tests for testing `target-sqlite` pipeline end-to-end by using precrafted test streams that are located in `tests/data_files/`. Those are extensive tests that run the full pipeline exactly in the same way as target-sqlite would run for the same configuration and inputs and then check the created tables and loaded data that everything went according to plan.


## Implementation Notes

There are some implicit decisions on the implementation of this Target:

*  Data are UPSERTed when an entity has at least one primary key (key_properties not empty). If there is already a row with the same
composite key (combination of key_properties) then the new record UPDATEs the existing one.

    In order for this TARGET to work on append only mode and the target tables to store historical information, no key_properties must be defined (the `config['timestamp_column']`'s value can be used to get the most recent information).

*  In order to support all versions of python 3.6+ and SQLite3.X, we are not using the UPSERT clauses (`INSERT .. ON CONFLICT ...`) that were added to SQLite with version 3.24.0 (2018-06-04). We instead try to insert each single record sent to `target-sqlite` and fall back to updating existing records in case an IntegrityError is detected (primary key or uniqueness violation).

    That means that `target-sqlite` is not as fast as it will be when SQLite 3.24+ is guaranteed to be bundled with python. This will be fixed in future versions of `target-sqlite`.

*  Even if there is no `config['timestamp_column']` attribute in the SCHEMA sent to `target-sqlite` for a specific stream, it is added explicitly. Each RECORD has the timestamp of when it was received by the Target as a value.

*  Schema updates are supported only for adding new columns as SQLite does not support updating existing columns.

    When a SCHEMA message for a stream is received, `target-sqlite` checks in the SQLite Database provided in the config whether there is already a table for the entity defined by the stream.
    * If there is no such table (or even schema), they are created.
    * If there is already a table for that entity, `target-sqlite` creates a diff in order to check if new attributes must be added to the table.
    * We never drop columns, only add new ones.

*  We unnest Nested JSON Data Structures and follow a `[object_name]__[property_name]` approach similar to [what Stitch platform also does](https://www.stitchdata.com/docs/data-structure/nested-data-structures-row-count-impact).

*  At the moment we do not deconstruct nested arrays. Arrays are stored as STRINGs with the relevant JSON representation stored as is. e.g. "['banana','apple']". You can check the related tests and test streams for how `target-sqlite` when arrays are sent in a stream.

*  All semi-structured data types (JSON objects) are stored as strings. You can check the related tests and test streams for how `target-sqlite` when semi-structured data are sent in a stream.

* [WAL](https://www.sqlite.org/wal.html) is now enabled by default on the database. This will enable multiple concurrent processes to access the database without locking out each other.
