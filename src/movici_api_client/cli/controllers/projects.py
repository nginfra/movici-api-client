from movici_api_client.cli.events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    GetAllProjects,
    GetSingleProject,
    UpdateProject,
    UploadProject,
)

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    cli_options,
    command,
    data_directory_option,
    format_output,
    handle_event,
    option,
)
from ..utils import prompt


class ProjectController(Controller):
    name = "project"
    decorators = (authenticated,)

    @command(name="projects", group="get")
    @format_output(fields=("uuid", "name", "display_name", "created_on"))
    @handle_event
    def list(self):
        return GetAllProjects()

    @command
    @argument("name_or_uuid")
    @format_output(fields=("uuid", "name", "display_name", "created_on"))
    @handle_event
    def get(self, name_or_uuid):
        return GetSingleProject(name_or_uuid)

    @command
    @argument("name")
    @option("--display-name")
    @format_output
    @handle_event
    def create(self, name, display_name):
        if display_name is None:
            display_name = prompt("Display name", default=name)
        return CreateProject(name, display_name)

    @command
    @argument("name_or_uuid")
    @option("--display-name")
    @format_output
    @handle_event
    def update(self, name_or_uuid, display_name):
        return UpdateProject(name_or_uuid, display_name)

    @command
    @argument("name_or_uuid")
    @format_output
    @handle_event
    def delete(self, name_or_uuid):
        return DeleteProject(name_or_uuid)

    @command
    @data_directory_option("project")
    @cli_options("overwrite", "create", "yes", "no", "inspect")
    @handle_event(success_message="Success!")
    def upload(self, directory):
        return UploadProject(directory)

    @command
    @data_directory_option(purpose="project")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download(self, directory):
        return DownloadProject(directory)
