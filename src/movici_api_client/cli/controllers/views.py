import pathlib

from movici_api_client.cli.events.view import (
    DeleteView,
    DownloadMultipleViews,
    DownloadView,
    DuplicateView,
    EditView,
    GetAllViews,
    GetSingleView,
    UploadMultipleViews,
    UploadView,
)

from ..common import Controller
from ..data_dir import DataDir
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
from ..exceptions import NotYetImplemented
from ..utils import FilePath


class ViewController(Controller):
    name = "view"
    decorators = (authenticated,)

    @command(name="views", group="get")
    @argument("scenario_name_or_uuid")
    @format_output(fields=("uuid", "name"))
    @handle_event
    def list(self, scenario_name_or_uuid):
        return GetAllViews(scenario_name_or_uuid)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @cli_options("output")
    @format_output(
        fields=(
            "uuid",
            "name",
        )
    )
    @handle_event
    def get(self, scenario_name_or_uuid, view_name_or_uuid):
        return GetSingleView(scenario_name_or_uuid, view_name_or_uuid)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @format_output
    def create(self, scenario_name_or_uuid, view_name_or_uuid):
        raise NotYetImplemented()

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @format_output
    def update(self, scenario_name_or_uuid, view_name_or_uuid):
        raise NotYetImplemented()

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @format_output
    @handle_event
    def delete(self, scenario_name_or_uuid, view_name_or_uuid):
        return DeleteView(scenario_name_or_uuid, view_name_or_uuid)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid", default="")
    @option("-f", "--file", type=FilePath(), required=True)
    @cli_options("overwrite", "create", "yes", "no")
    @handle_event(success_message="Success!")
    def upload(self, scenario_name_or_uuid, view_name_or_uuid, file: pathlib.Path):
        return UploadView(scenario_name_or_uuid, view_name_or_uuid, file=file)

    @command(name="views", group="upload")
    @argument("scenario_name_or_uuid")
    @data_directory_option(purpose="views")
    @cli_options("overwrite", "create", "yes", "no")
    @handle_event(success_message="Success!")
    def upload_multiple(self, scenario_name_or_uuid, directory):
        return UploadMultipleViews(scenario_name_or_uuid, directory)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @data_directory_option(purpose="views")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download(self, scenario_name_or_uuid, view_name_or_uuid, directory: DataDir):
        return DownloadView(scenario_name_or_uuid, view_name_or_uuid, directory=directory)

    @command(name="views", group="download")
    @argument("scenario_name_or_uuid")
    @data_directory_option(purpose="views")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download_multiple(self, scenario_name_or_uuid, directory):
        return DownloadMultipleViews(scenario_name_or_uuid, directory=directory)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @handle_event(success_message="Succesfully updated view")
    def edit(self, scenario_name_or_uuid, view_name_or_uuid):
        return EditView(scenario_name_or_uuid, view_name_or_uuid)

    @command
    @argument("scenario_name_or_uuid")
    @argument("view_name_or_uuid")
    @option("--name", required=True)
    @handle_event(success_message="View succesfully duplicated")
    def duplicate(self, scenario_name_or_uuid, view_name_or_uuid, name):
        return DuplicateView(scenario_name_or_uuid, view_name_or_uuid, new_view_name=name)
