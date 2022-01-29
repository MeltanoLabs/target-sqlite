import argparse
import io
import json
import sys
import singer

from jsonschema import ValidationError
from sqlalchemy.exc import DatabaseError

from target_sqlite.target_sqlite import TargetSQLite
from target_sqlite.utils.error import SchemaUpdateError

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "database",
]


def process_input(config, lines):
    """
    The core processing loop for any Target

    Loop through the lines sent in sys.stdin, process each one and run DDL and
    batch DML operations.
    """
    target = TargetSQLite(config)

    # Loop over lines from stdin
    for line in lines:
        target.process_line(line)

    # If the tap finished its execution, flush the records for any remaining
    #  streams that still have records cached (i.e. row_count < batch_size)
    target.flush_all_cached_records()


def main_implementation():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=True)
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        raise Exception("No config file provided")

    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))

    # Run the Input processing loop until everything is done
    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    process_input(config, input)

    LOGGER.debug("Exiting normally")


def main():
    try:
        # wrap the real main() and catch exceptions we want to handle somehow
        main_implementation()
    except (ValidationError, DatabaseError, SchemaUpdateError) as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
