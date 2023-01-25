from unittest.mock import AsyncMock
from uuid import UUID

import gimme
import pytest

from movici_api_client.api.client import AsyncClient
from movici_api_client.api.requests import GetDatasets, GetProjects, GetScenarios
from movici_api_client.cli.handlers.query import (
    DatasetQuery,
    ProjectQuery,
    ResourceQuery,
    ScenarioQuery,
)


class DummyResourceQuery(ResourceQuery):
    def request_all(self):
        return None


def uuid(int: int):
    return str(UUID(int=int))


@pytest.fixture
def mock_client(setup_gimme):
    client = gimme.that(AsyncClient)
    client.request.return_value = [
        {"name": "resource_a", "uuid": uuid(1)},
        {"name": "resource_b", "uuid": uuid(2)},
    ]
    return client


@pytest.fixture(autouse=True)
def setup_gimme(gimme_repo):
    gimme_repo.register(AsyncClient, factory=lambda: AsyncMock(AsyncClient), store=True)


@pytest.mark.parametrize(
    "query,request_cls",
    [
        (ProjectQuery(), GetProjects),
        (DatasetQuery(""), GetDatasets),
        (ScenarioQuery(""), GetScenarios),
    ],
)
def test_request_all(query, request_cls):
    assert isinstance(query.request_all(), request_cls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name_or_uuid, result",
    [
        ("resource_a", uuid(1)),
        ("resource_b", uuid(2)),
        (uuid(1), uuid(1)),
    ],
)
async def test_get_uuid(name_or_uuid, result, mock_client):
    query = DummyResourceQuery()
    assert await query.get_uuid(name_or_uuid) == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name_or_uuid, result",
    [
        ("resource_a", {"name": "resource_a", "uuid": uuid(1)}),
        ("resource_b", {"name": "resource_b", "uuid": uuid(2)}),
        (uuid(1), {"name": "resource_a", "uuid": uuid(1)}),
    ],
)
async def test_by_name_or_uuid(name_or_uuid, result, mock_client):
    query = DummyResourceQuery()
    assert await query.by_name_or_uuid(name_or_uuid) == result
