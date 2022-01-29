import json
import singer
import sys

from datetime import datetime
from jsonschema import ValidationError, Draft4Validator, FormatChecker
from typing import Dict, List, Iterator

from target_sqlite.utils.singer_target_utils import (
    flatten_record,
    flatten_key,
    generate_sqlalchemy_table,
)
from target_sqlite.sqlite_loader import SQLiteLoader


LOGGER = singer.get_logger()


class RecordBuffer(list):
    def add_record(self, record: Dict):
        self.append(record)

    def values(self):
        return self


class UniqueRecordBuffer(dict):
    def __init__(self, key_func=lambda x: x):
        self.key = key_func

    def add_record(self, record: Dict):
        self[self.key(record)] = record

    def values(self):
        return list(super().values())

    def __iter__(self):
        for record in self.values():
            yield record


class StateBuffer:
    """
    A Buffer to store all state messages as we receive them, so that we can
    flush them to stdout the moment all their relevant streams are flushed.

    The Singer.io specification allows full freedom to each tap on what to store
    in its STATE messages. So, without insight to each Tap's business logic, the
    only way for a target to be sure that a STATE message is ready to be flushed
    to stdoud, is to wait for all RECORDS that have arrived before the STATE
    message to be processed and then flush the STATE message.

    The idea is that we store the STATE messages ordered in a State Buffer,
    together with all the unflushed streams the moment it was received.

    Each time a stream is flushed, we also update all the relevant streams for
    the STATE messages stored in the StateBuffer and then check if any STATE
    messages have no more any unflushed streams associated with them.

    Those are streams that can be safely flushed to stdout and, following the
    Singer.io specification, we flush only the most recent one, as it should
    have the most up to date information on the state of the Tap.
    """

    def __init__(self) -> None:
        self.buffer = []

    def add_state(self, state: str, streams: List) -> None:
        LOGGER.debug(f"StateBuffer: new state stored {state}: {streams}")
        self.buffer.append({"state": state, "streams": streams})

    def flush_stream(self, stream: str) -> None:
        for state in self.buffer:
            state["streams"] = [x for x in state["streams"] if x != stream]

    def pop_states_without_streams(self) -> List[str]:
        states = [state["state"] for state in self.buffer if not state["streams"]]
        self.buffer = [state for state in self.buffer if state["streams"]]
        return states

    def __iter__(self):
        for state in self.buffer:
            yield state


class TargetSQLite:
    def __init__(self, config: Dict) -> None:
        # Store the Config so that we can use it to initiate SQLite Loaders
        #  for various tables
        self.config: Dict = config
        self.batch_size = int(config.get("batch_size", 50))
        self.timestamp_column = config.get("timestamp_column", "__loaded_at")

        # Store all the state messages in a State Buffer, so that we can flush
        #  them to stdout the moment all their relevant streams are flushed
        self.states = StateBuffer()
        # Also store the last emitted state for reference and for facilitating tests
        self.last_emitted_state = None

        # Keep track of the streams we have schemas for.
        # A tap sending a record without previously describing its schema is not
        #  properly following the Singer.io Spec
        # The schemas variable is only used for lookups as the SQLiteLoader
        #  for that stream with all the schema info and the connection options
        #  is stored for each stream in loaders
        self.schemas: List = []
        self.loaders: Dict = {}

        # Also keep track of a template empty record for each stream in order
        #  to map all incoming records against and normalize them to use their
        #  fully defined schema
        self.template_records: Dict = {}

        # Finaly, keep the attributes of the database Table associated with
        #  each stream for quick lookups.
        # It is used while flattening records in order to know when an attribute
        #  is defined as an Object (i.e. semistructured data type) and its values
        #  must be stored as they are without further unnesting them.
        self.entity_attributes: Dict = {}

        # The key_properties has the keys for each stream to enable quick
        #  lookups during schema validation of each received record
        #  (all keys should be there even if they are not marked as required)
        self.key_properties: Dict = {}

        # For each stream, also keep a schema JSON Schema validator to validate
        #  new records against
        self.validators: Dict = {}

        # Cache the records for each stream in rows[stream]
        # When the cache reaches the batch_size or when the tap stops
        #  sending data, we flush the cached records (i.e. send them in batch to
        #  SQLite). This is important for performance: we don't want to send
        #  an insert with each record received.
        self.rows: Dict = {}

    def extract_keys(self, stream: str, record: Dict):
        return tuple(record[key] for key in self.key_properties[stream])

    def process_line(self, line: str) -> None:
        """
        Process a Singer.io Message, which is provided in a single line
        """
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(line))
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )

            stream = o["stream"]
            if stream not in self.schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        stream
                    )
                )

            # Validate record against the schema for that stream
            flat_record = self.validate_record(
                stream, o["record"], self.key_properties[stream]
            )

            # Add an `timestamp_column` timestamp for the record
            if self.timestamp_column not in flat_record:
                flat_record[self.timestamp_column] = datetime.utcnow()

            # Normalize the record to make sure it follows the full schema defined
            new_record = self.template_records[stream].copy()
            new_record.update(flat_record)

            # Store the record so that we can load in batch_size batches
            self.rows[stream].add_record(new_record)

            # If the batch_size has been reached for this stream, flush the records
            if len(self.rows[stream]) >= self.batch_size:
                self.flush_records(stream)
        elif t == "STATE":
            new_state = o["value"]
            unflushed_streams = list(self.streams_with_unflushed_records())

            if unflushed_streams:
                # There are unflushed streams --> store the STATE message in StateBuffer
                self.states.add_state(new_state, unflushed_streams)
            else:
                # All streams are clean, no cached records at the moment
                # Just send the STATE message directly to stdout
                self.emit_state(new_state)
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )

            stream = o["stream"]

            # Reject the valid JSON schema with no properties.
            # SQLite Target has to map any input to a relational schema,
            #  which means that at least one attribute, even if it is a
            #  semistructured object, must be present in order to populate
            #  the relational table to be created.
            if "properties" not in o["schema"]:
                raise ValidationError(
                    f"Not supported schema by target-sqlite:\n {line}\n"
                    "It should at least have one top level property in schema."
                )

            if stream in self.schemas:
                # We received a new Schema message for a stream that already
                #  has a Schema defined.
                # Flush the cached records as we may have an updated Schema
                #  going forward that will be incompatible with the current one
                self.flush_records(stream)
            else:
                # The Schema message is for a newly encountered stream
                # Record that the schema for this stream has been received
                self.schemas.append(stream)

            # Add a validator based on the received JSON Schema
            self.validators[stream] = Draft4Validator(
                o["schema"], format_checker=FormatChecker()
            )

            # We could live without it for append only use cases without a key,
            #  but it is part of the Singer.io SPEC
            if "key_properties" not in o:
                raise Exception("key_properties field is required")

            # We have to process the `key_properties` like all columns
            key_properties = [flatten_key(prop, [], "") for prop in o["key_properties"]]

            # Store the Key properties for quick lookups during record validation
            self.key_properties[stream] = key_properties

            # Generate an sqlalchemy Table based on the info received
            # It is used to store and access all the schema information
            #  in a structured way
            sqlalchemy_table = generate_sqlalchemy_table(
                stream, key_properties, o["schema"], self.timestamp_column
            )

            # Create a SQLiteLoader for that sqlalchemy Table and
            #  run schema_apply() to create the Schema and/or Table if they
            #  are not there.
            loader = SQLiteLoader(table=sqlalchemy_table, config=self.config)

            try:
                loader.schema_apply()
            except Exception as exc:
                LOGGER.error(
                    "Exception in schema_apply() while prrocessing:\n{}".format(line)
                )
                raise exc

            # This buffering makes sure that if we receive multiple rows that
            #  would violate the `key_properties` uniqueness,
            #  only the last one will be kept.
            if key_properties:
                self.rows[stream] = UniqueRecordBuffer(
                    lambda record: self.extract_keys(stream, record)
                )
            else:
                self.rows[stream] = RecordBuffer()

            # Keep a template empty record for each stream in order to map
            #  all incoming records against
            self.template_records[stream] = loader.empty_record()

            # Keep the loader in loaders[stream] to be used for loading the
            #  records received for this stream.
            self.loaders[stream] = loader

            # And also keep the attributes of the database Table associated
            #  with this stream
            self.entity_attributes[stream] = loader.attribute_names()
        elif t == "ACTIVATE_VERSION":
            # No support for that type of message yet
            LOGGER.warn("ACTIVATE_VERSION message")
        else:
            LOGGER.warn("Skipping unknown message type {}.".format(o["type"]))

    def validate_record(self, stream: str, record: Dict, keys: List) -> Dict:
        """
        Validate a record against the schema for its stream

        Checks that:
        1. The record follows the JSON schema of the SCHEMA message
        2. All the keys are present even if they are not market as required in
             the JSON schema

        Returns the flattened record ready for integration
        """
        self.validators[stream].validate(record)
        flat_record = flatten_record(record, self.entity_attributes[stream])
        missing_keys = [key for key in keys if key not in flat_record]

        if missing_keys:
            raise ValidationError(
                f"Record {record} is missing key properties {missing_keys}"
            )

        return flat_record

    def flush_all_cached_records(self) -> None:
        """
        Flush the records for any remaining streams that still have
        records cached (i.e. row_count < batch_size)
        """
        to_flush = self.streams_with_unflushed_records()

        for stream in to_flush:
            self.flush_records(stream)

    def flush_records(self, stream: str) -> None:
        """
        Flush the cached records stored in rows[stream] for a specific stream.

        loaders[stream] has an initialized SQLiteLoader for the table defined
        by the schema we have received for that stream.
        """

        # Load the data
        self.loaders[stream].load(self.rows[stream].values())

        # Clear the cached records and reset the counter for the stream
        self.rows[stream].clear()

        # Mark the stream as flushed in StateBuffer
        #  and check if there are any STATE messages ready to be also flushed
        self.states.flush_stream(stream)
        states_without_streams = self.states.pop_states_without_streams()

        if states_without_streams:
            # Only write the most resent state
            self.emit_state(states_without_streams.pop())

    def emit_state(self, state) -> None:
        """
        Emit the given state to stdout
        """
        if state is not None:
            line = json.dumps(state)
            LOGGER.debug('Emitting state {}'.format(line))
            sys.stdout.write("{}\n".format(line))
            sys.stdout.flush()

            self.last_emitted_state = state

    def streams_with_unflushed_records(self) -> Iterator[str]:
        """
        Return all the streams that have records cached.

        Used in order to:
        (a) get all the streams to flush when the execution ends
        (b) when receiving a STATE message, in order to identify 'dirty'
            streams that must be flushed before emiting the STATE to stdout.
        """
        return (stream for (stream, rows) in self.rows.items() if len(rows))
