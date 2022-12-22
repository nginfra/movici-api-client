import pytest

from movici_api_client.api.client import parse_service_urls
from movici_api_client.api.common import Service


@pytest.mark.parametrize(
    "urls,prefix,expected",
    [
        (None, "", Service.urls()),
        ({}, "", Service.urls()),
        (
            {"service.auth": "/some/auth"},
            "service.",
            {
                Service.AUTH: "/some/auth",
                Service.DATA_ENGINE: Service.DATA_ENGINE.value[1],
                Service.MODEL_ENGINE: Service.MODEL_ENGINE.value[1],
            },
        ),
        (
            {
                "service.auth": "/some/auth",
                "service.data_engine": None,
                "service.model_engine": None,
            },
            "service.",
            {Service.AUTH: "/some/auth"},
        ),
    ],
)
def test_get_service_urls(urls, prefix, expected):
    assert parse_service_urls(urls, prefix=prefix) == expected
