from unittest.mock import patch

import pytest

import movici_api_client.cli.handlers.remote
from movici_api_client.cli.config import Context
from movici_api_client.cli.cqrs import Event, EventHandler, Mediator
from movici_api_client.cli.events.project import GetAllProjects
from movici_api_client.cli.handlers.remote import requires_valid_project_uuid


@pytest.fixture
def current_context():
    with patch.object(movici_api_client.cli.handlers.remote, "assert_current_context") as mock:
        mock.return_value = Context("dummy", url="", project="some_project")
        yield mock


class DummyGetAllProjectsHandler(EventHandler):
    async def handle(self, event: Event, mediator: Mediator):
        return [{"name": "some_project", "uuid": "0000-0001"}]


@pytest.mark.asyncio
async def test_requires_valid_project_uuid(current_context):
    mediator = Mediator({GetAllProjects: DummyGetAllProjectsHandler})

    @requires_valid_project_uuid
    class MyHandler(EventHandler):
        project_uuid: str

        async def handle(self, event: Event, mediator: Mediator):
            pass

    handler = MyHandler()

    await handler.handle(object(), mediator)
    assert handler.project_uuid == "0000-0001"
