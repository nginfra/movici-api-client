import json
import os

import gimme
import pytest

from movici_api_client.cli.config import CONFIG_LOCATION_ENV, get_config
from movici_api_client.cli.testing import FakeClient


@pytest.fixture(autouse=True)
def gimme_repo():
    with gimme.context() as ctx:
        yield ctx


@pytest.fixture(autouse=True)
def client(gimme_repo):
    client = FakeClient()
    gimme_repo.add(client)
    return client


@pytest.fixture(autouse=True)
def config_path(tmp_path):
    file = tmp_path / ".movici.conf"
    os.environ[CONFIG_LOCATION_ENV] = str(file)
    file.write_text(
        json.dumps(
            {
                "version": 1,
                "current_context": "test_context",
                "contexts": [
                    {
                        "name": "test_context",
                        "url": "https://example.org",
                    }
                ],
            }
        )
    )

    return file


@pytest.fixture
def read_config(config_path):
    def read_config_():
        return get_config(config_path)

    return read_config_


@pytest.fixture(autouse=True)
def reset_dependencies():
    yield
