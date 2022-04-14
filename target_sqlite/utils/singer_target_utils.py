import inflection
import itertools
import json
import logging
import re
from collections.abc import MutableMapping

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.types import TIMESTAMP, Float, String, BigInteger, Boolean

# Set of helper functions for flattening records and schemas.
# The core ones are:
# + flatten_record(record, schema)
#   Flatten (un-nest) a given data record according to a schema.
#   e.g. {"id": 3, "info": {"weather": "sunny", "mood": "happy"}}}
#     --> {"id": 3, "info__weather": "sunny", "info__mood": "happy"}
# + flatten_schema(json_schema_definition) --> flatten a given json schema.
# + generate_sqlalchemy_table(stream, key_properties, json_schema, timestamp_column)
#    --> Generate an sqlalchemy Table based on a SCHEMA message
logger = logging.getLogger()
logger.setLevel(logging.WARNING)


def generate_sqlalchemy_table(stream, key_properties, json_schema, timestamp_column):
    flat_schema = flatten_schema(json_schema)
    schema_dict = {
        Column(name, sqlalchemy_column_type(schema), primary_key=True)
        for (name, schema) in flat_schema.items()
    }

    columns = []
    for (name, schema) in flat_schema.items():
        pk = name in key_properties
        column = Column(name, sqlalchemy_column_type(schema), primary_key=pk)
        columns.append(column)

    if timestamp_column and timestamp_column not in flat_schema:
        column = Column(timestamp_column, TIMESTAMP)
        columns.append(column)

    # Replace all special characters and CamelCase with underscores
    table_name = re.sub("[^0-9a-zA-Z_]+", "_", stream)
    table_name = inflection.underscore(table_name)
    table = Table(table_name, MetaData(), *columns)

    return table


def inflect_column_name(name):
    name = re.sub(r"([A-Z]+)_([A-Z][a-z])", r"\1__\2", name)
    name = re.sub(r"([a-z\d])_([A-Z])", r"\1__\2", name)
    # Also replace all special characters and CamelCase with underscores
    name = re.sub("[^0-9a-zA-Z_]+", "_", name)
    return inflection.underscore(name)


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = [inflect_column_name(n) for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 63 and reducer_index < len(inflected_key):
        reduced_key = re.sub(
            r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
        )
        inflected_key[reducer_index] = (
            reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]
        ).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_record(d, schema, parent_key=[], sep="__"):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)

        if new_key in schema:
            # If the attribute name (new_key) is defined in the schema
            # Then stop un-nesting and store its values as they are even if
            #  it is an object
            if isinstance(v, MutableMapping) or isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        elif isinstance(v, MutableMapping):
            items.extend(flatten_record(v, schema, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if isinstance(v, list) else v))
    return dict(items)


def flatten_schema(d, parent_key=[], sep="__"):
    items = []
    if "properties" in d.keys():
        for k, v in d["properties"].items():
            new_key = flatten_key(k, parent_key, sep)

            if not v:
                logger.warn("Empty definition for {}.".format(new_key))
                continue

            if "type" in v.keys():
                if "object" in v["type"]:
                    # Additional check that objects without properties are allowed
                    if "properties" in v.keys():
                        items.extend(
                            flatten_schema(v, parent_key + [k], sep=sep).items()
                        )
                    else:
                        # An object without properties (for semistructured data)
                        items.append((new_key, v))
                else:
                    items.append((new_key, v))
            else:
                property = list(v.values())[0][0]
                if property["type"] == "string":
                    property["type"] = ["null", "string"]
                    items.append((new_key, property))
                elif property["type"] == "array":
                    property["type"] = ["null", "array"]
                    items.append((new_key, property))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError("Duplicate column name produced in schema: {}".format(k))

    return dict(sorted_items)


def sqlalchemy_column_type(schema_property):
    property_type = schema_property["type"]
    property_format = schema_property["format"] if "format" in schema_property else None

    if "object" in property_type:
        return String  # OBJECT
    elif "array" in property_type:
        return String  # ARRAY
    elif property_format == "date-time":
        return String
    elif "number" in property_type:
        return Float
    elif "integer" in property_type and "string" in property_type:
        return String
    elif "integer" in property_type:
        return BigInteger
    elif "boolean" in property_type:
        return Boolean
    else:
        return String


# from sqlalchemy.dialects.sqlite import FLOAT, INTEGER, BOOLEAN, TIMESTAMP, VARCHAR
