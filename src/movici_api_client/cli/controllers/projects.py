import asyncio

from movici_api_client.api.requests import (
    CreateProject,
    DeleteProject,
    GetProjects,
    GetSingleProject,
)
from movici_api_client.cli.filetransfer.download import DownloadProject
from movici_api_client.cli.filetransfer.upload import UploadProject

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    catch_exceptions,
    combine_decorators,
    command,
    data_directory_option,
    download_options,
    format_output,
    option,
    upload_options,
    valid_project_uuid,
)
from ..exceptions import InvalidUsage, NotYetImplemented
from ..ui import format_object
from ..utils import assert_project_uuid, confirm, echo, maybe_set_flag, prompt, validate_uuid


def upload_project_options(func):
    return combine_decorators(
        [
            option(
                "-i",
                "--inspect",
                is_flag=True,
                help="read files to infer meta data and enforce consistency",
            ),
        ]
    )(upload_options(func))


class ProjectController(Controller):
    name = "project"

    decorators = (authenticated, catch_exceptions)

    @command(name="projects", group="get")
    @format_output(fields=("uuid", "name", "display_name", "created_on"))
    def list(self):
        return self.client.request(GetProjects())

    @command
    @argument("name_or_uuid")
    def get(self, name_or_uuid):

        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
        else:
            uuid = assert_project_uuid(name_or_uuid)

        result = self.client.request(GetSingleProject(uuid))
        echo(format_object(result, fields=("uuid", "name", "display_name", "created_on")))

    @command
    @argument("name")
    @option("--display_name")
    @format_output
    def create(self, name, display_name):
        if display_name is None:
            display_name = prompt("Display name", default=name)
        return self.client.request(CreateProject(name, display_name))

    @command
    def update(self):
        raise NotYetImplemented()

    @command
    @argument("name_or_uuid")
    @format_output
    def delete(self, name_or_uuid):

        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
        else:
            uuid = assert_project_uuid(name_or_uuid)

        confirm(
            f"Are you sure you wish to delete project '{name_or_uuid}'"
            " with all its associated data?"
        )
        return self.client.request(DeleteProject(uuid))

    @command
    @valid_project_uuid
    @data_directory_option("project")
    @upload_project_options
    def upload(self, project_uuid, directory, overwrite, create, yes, no, inspect):
        if yes and no:
            raise InvalidUsage("cannot combine --yes with --no")
        self.params.overwrite = maybe_set_flag(overwrite, yes, no)
        self.params.create = maybe_set_flag(create, yes, no)
        self.params.inspect = inspect
        self.params.with_simulation = True
        self.params.with_views = True
        asyncio.run(UploadProject(directory, uuid=project_uuid).run())
        echo("Success")

    @command
    @valid_project_uuid
    @download_options("project")
    def download(self, project_uuid, directory, overwrite, no_overwrite):

        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")
        self.params.overwrite = maybe_set_flag(
            False, default_yes=overwrite, default_no=no_overwrite
        )
        self.params.with_simulation = True
        self.params.with_views = True
        directory.initialize()
        asyncio.run(DownloadProject(parent={"uuid": project_uuid}, directory=directory).run())

        echo("Success!")
