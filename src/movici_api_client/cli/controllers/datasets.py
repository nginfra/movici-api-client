import pathlib

from movici_api_client.cli.events.dataset import (
    ClearDataset,
    CreateDataset,
    DeleteDataset,
    DownloadDataset,
    DownloadMultipleDatasets,
    EditDataset,
    GetAllDatasets,
    GetSingleDataset,
    UpdateDataset,
    UploadDataset,
    UploadMultipleDatasets,
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
from ..utils import FilePath, prompt


class DatasetController(Controller):
    name = "dataset"
    decorators = (authenticated,)

    @command(name="datasets", group="get")
    @cli_options("inspect")
    @format_output(
        fields=(
            "uuid",
            "name",
            "display_name",
            "type",
            "has_data",
        )
    )
    @handle_event
    def list(self):
        return GetAllDatasets()

    @command
    @argument("name_or_uuid")
    @format_output(
        fields=(
            "uuid",
            "name",
            "display_name",
            "type",
            "format",
            "has_data",
        )
    )
    @handle_event
    def get(self, name_or_uuid):
        return GetSingleDataset(name_or_uuid)

    @command
    @argument("name")
    @option("--display_name")
    @option("dataset_type", "--type")
    @format_output
    @handle_event
    def create(self, name, display_name, dataset_type):
        if display_name is None:
            display_name = prompt("Display name", default=name)
        return CreateDataset(name=name, display_name=display_name, type=dataset_type)

    @command
    @argument("name_or_uuid")
    @option("--name", help="New Dataset name")
    @option("--display-name", help="New display name")
    @option("--type", help="New dataset type", default=None)
    @format_output
    @handle_event
    def update(self, name_or_uuid, name, display_name, type):
        return UpdateDataset(
            name_or_uuid=name_or_uuid, name=name, display_name=display_name, type=type
        )

    @command
    @argument("name_or_uuid")
    @format_output
    @handle_event
    def delete(self, name_or_uuid):
        return DeleteDataset(name_or_uuid)

    @command
    @argument("name_or_uuid")
    @format_output
    @handle_event
    def clear(self, name_or_uuid):
        return ClearDataset(name_or_uuid)

    @command
    @argument("name_or_uuid", default="")
    @option("-f", "--file", type=FilePath(), required=True)
    @cli_options("overwrite", "create", "yes", "no", "inspect")
    @handle_event(success_message="Success!")
    def upload(self, name_or_uuid, file: pathlib.Path):
        return UploadDataset(name_or_uuid, file)

    @command(name="datasets", group="upload")
    @data_directory_option(purpose="datasets")
    @cli_options("overwrite", "create", "yes", "no", "inspect")
    @handle_event(success_message="Success!")
    def upload_multiple(self, directory):
        return UploadMultipleDatasets(directory)

    @command
    @argument("name_or_uuid")
    @data_directory_option(purpose="datasets")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download(self, name_or_uuid, directory):
        return DownloadDataset(name_or_uuid, directory)

    @command(name="datasets", group="download")
    @data_directory_option(purpose="datasets")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download_multiple(self, directory):
        return DownloadMultipleDatasets(directory)

    @command
    @argument("name_or_uuid")
    @handle_event(success_message="Succesfully updated dataset")
    def edit(self, name_or_uuid):
        return EditDataset(name_or_uuid)
