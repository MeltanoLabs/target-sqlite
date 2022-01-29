"""Tests standard tap features using the built-in SDK tests library."""

from typing import Any, Dict

from singer_sdk.testing import get_standard_tap_tests

from singer_sqlite import SQLiteTap

SAMPLE_CONFIG: Dict[str, Any] = {
    # Tap config for tests are loaded from env vars (see `.env.template`)
    # "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(sqlite_sample_db_config):
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(SQLiteTap, config=sqlite_sample_db_config)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
