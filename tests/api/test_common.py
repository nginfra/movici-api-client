from unittest.mock import Mock
from movici_api_client.api.common import urljoin
import pytest
import httpx


@pytest.fixture
def session():
    return Mock(spec=httpx.Client)


@pytest.mark.parametrize(
    "parts, expected",
    [
        (("https://example.org", "part1", "part2"), "https://example.org/part1/part2/"),
        (("", "part"), "/part/"),
        (("/", "part"), "/part/"),
    ],
)
def test_urljoin(parts, expected):
    assert urljoin(*parts) == expected
