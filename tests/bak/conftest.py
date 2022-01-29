import json
import pytest


class SilencedDict(dict):
    def __repr__(self):
        return "Dict[ ... sensitive_data ... ]"

    def __str__(self):
        return "Dict[ ... sensitive_data ... ]"


def pytest_addoption(parser):
    parser.addoption("--config", help="Config file", required=True)


@pytest.fixture(scope="session")
def config(request):
    config = SilencedDict()

    config_file = request.config.getoption("--config")

    if config_file:
        with open(config_file) as input:
            config.update(json.load(input))
    else:
        raise Exception("No config file provided")

    config["timestamp_column"] = config.get("timestamp_column", "__loaded_at")

    return config
