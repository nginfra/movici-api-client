from movici_api_client.api.client import Client
from movici_api_client.api.requests import (
    CreateProject,
    DeleteProject,
    GetProjects,
    GetSingleProject,
)
from ..dependencies import get
from ..exceptions import NotYetImplemented
from ..ui import format_object

from ..common import Controller
from ..decorators import argument, authenticated, command, option, format_output
from ..utils import DirPath, assert_project_uuid, echo, validate_uuid, prompt, confirm


class ProjectController(Controller):
    name = "project"

    decorators = (authenticated,)

    @command(name="projects")
    @format_output(fields=("uuid", "name", "display_name", "created_on"))
    def list(self):
        client = get(Client)
        return client.request(GetProjects())

    @command
    @argument("name_or_uuid")
    def get(self, name_or_uuid):
        client = get(Client)

        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
        else:
            uuid = assert_project_uuid(name_or_uuid)

        result = client.request(GetSingleProject(uuid))
        echo(format_object(result, fields=("uuid", "name", "display_name", "created_on")))

    @command
    @argument("name")
    @option("--display_name")
    @format_output
    def create(self, name, display_name):
        if display_name is None:
            display_name = prompt("Display name", default=name)
        client = get(Client)
        return client.request(CreateProject(name, display_name))

    @command
    def update(self):
        raise NotYetImplemented()

    @command
    @argument("name_or_uuid")
    @format_output
    def delete(self, name_or_uuid):
        client = get(Client)

        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
        else:
            uuid = assert_project_uuid(name_or_uuid)

        confirm(
            f"Are you sure you wish to delete project '{name_or_uuid}'"
            " with all its associated data?"
        )
        return client.request(DeleteProject(uuid))

    @command
    @argument("project")
    @option("-d", "--directory", type=DirPath())
    def upload(self, project, directory):
        raise NotYetImplemented()
