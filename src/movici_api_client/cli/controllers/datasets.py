import asyncio
import pathlib

from movici_api_client.api.client import AsyncClient, Client
from movici_api_client.api.requests import (
    CreateDataset,
    DeleteDataset,
    DeleteDatasetData,
    GetDatasetData,
    GetDatasets,
    GetDatasetTypes,
    GetSingleDataset,
    UpdateDataset,
)
from movici_api_client.cli.filetransfer.upload import UploadResource

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    combine_decorators,
    command,
    download_options,
    format_output,
    option,
    upload_options,
    valid_project_uuid,
)
from ..dependencies import get
from ..exceptions import InvalidUsage, NoChangeDetected
from ..filetransfer import (
    DatasetUploadStrategy,
    DownloadDatasets,
    DownloadResource,
    UploadMultipleResources,
)
from ..helpers import edit_resource
from ..utils import DirPath, FilePath, confirm, echo, maybe_set_flag, prompt, prompt_choices
from .common import get_dataset, get_dataset_uuid, resolve_data_directory


def upload_dataset_options(func):
    return combine_decorators(
        [
            option(
                "-i",
                "--inspect",
                is_flag=True,
                help="Try to read files to determine their dataset type, less performant",
            ),
        ]
    )(upload_options(func))


class DatasetController(Controller):
    name = "dataset"
    decorators = (valid_project_uuid, authenticated)

    @command(name="datasets", group="get")
    @format_output(
        fields=(
            "uuid",
            "name",
            "display_name",
            "type",
            "has_data",
        )
    )
    def list(self, project_uuid):
        client = get(Client)
        return client.request(GetDatasets(project_uuid))

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
    def get(self, project_uuid, name_or_uuid):
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)

        return client.request(GetSingleDataset(uuid))

    @command
    @argument("name")
    @option("--display_name")
    @option("dataset_type", "--type")
    @format_output
    def create(self, project_uuid, name, display_name, dataset_type):
        client = get(Client)
        if display_name is None:
            display_name = prompt("Display name", default=name)
        if dataset_type is None:
            all_types = client.request(GetDatasetTypes())
            dataset_type = prompt_choices("Type", sorted([tp["name"] for tp in all_types]))
        return client.request(
            CreateDataset(project_uuid, name=name, type=dataset_type, display_name=display_name)
        )

    @command
    @argument("name_or_uuid")
    @option("--name", help="New Dataset name")
    @option("--display_name", help="New display name")
    @option("--type", help="New dataset type", default=None)
    @format_output
    def update(self, project_uuid, name_or_uuid, name, display_name, type):
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)
        if not any((name, type, display_name)):
            raise NoChangeDetected()
        return client.request(UpdateDataset(uuid, name=name, type=type, display_name=display_name))

    @command
    @argument("name_or_uuid")
    @format_output
    def delete(self, project_uuid, name_or_uuid):
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)

        confirm(f"Are you sure you wish to delete dataset '{name_or_uuid}' and all its data?")
        return client.request(DeleteDataset(uuid))

    @command
    @argument("name_or_uuid")
    @format_output
    def clear(self, project_uuid, name_or_uuid):
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)

        confirm(f"Are you sure you wish to clear dataset '{name_or_uuid}' of all data?")
        return client.request(DeleteDatasetData(uuid))

    @command
    @argument("name_or_uuid", default="")
    @option("-f", "--file", type=FilePath(), required=True)
    @upload_dataset_options
    def upload(
        self, project_uuid, name_or_uuid, file: pathlib.Path, overwrite, create, yes, no, inspect
    ):
        name_or_uuid = name_or_uuid or file.stem
        client = AsyncClient.from_sync_client(get(Client))
        asyncio.run(
            UploadResource(
                client,
                file,
                parent_uuid=project_uuid,
                strategy=DatasetUploadStrategy(client),
                name_or_uuid=name_or_uuid,
                overwrite=maybe_set_flag(overwrite, yes, no),
                create_new=maybe_set_flag(create, yes, no),
                inspect_file=inspect,
            ).run()
        )

        echo("Success!")

    @command(name="datasets", group="upload")
    @option(
        "-d",
        "--directory",
        type=DirPath(writable=True),
        default=None,
        callback=lambda _, __, path: resolve_data_directory(path, "datasets"),
    )
    @upload_dataset_options
    def upload_multiple(self, project_uuid, directory, overwrite, create, yes, no, inspect):
        if yes and no:
            raise InvalidUsage("cannot combine --force with --never")
        client = AsyncClient.from_sync_client(get(Client))
        strategy = DatasetUploadStrategy(client=client)

        asyncio.run(
            UploadMultipleResources(
                client,
                directory,
                project_uuid,
                strategy=strategy,
                overwrite=maybe_set_flag(overwrite, yes, no),
                create_new=maybe_set_flag(create, yes, no),
                inspect_file=inspect,
            ).run()
        )
        echo("Success!")

    @command
    @argument("name_or_uuid")
    @download_options(purpose="datasets")
    def download(self, project_uuid, name_or_uuid, directory, overwrite, no_overwrite):
        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")
        client = get(Client)
        dataset = get_dataset(name_or_uuid, project_uuid, client)

        client = AsyncClient.from_sync_client(client)
        asyncio.run(
            DownloadResource(
                client=client,
                directory=directory,
                name=dataset["name"],
                request=GetDatasetData(dataset["uuid"]),
                overwrite=maybe_set_flag(False, default_yes=overwrite, default_no=no_overwrite),
            ).run()
        )
        echo("Success!")

    @command(name="datasets", group="download")
    @download_options(purpose="datasets")
    def download_multiple(self, project_uuid, directory, overwrite, no_overwrite):
        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")
        client = get(Client)
        client = AsyncClient.from_sync_client(client)
        asyncio.run(
            DownloadDatasets(
                {"uuid": project_uuid},
                client,
                directory=directory,
                overwrite=maybe_set_flag(False, default_yes=overwrite, default_no=no_overwrite),
            ).run()
        )
        echo("Success!")

    @command
    @argument("name_or_uuid")
    def edit(self, project_uuid, name_or_uuid):
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)
        current = client.request(GetSingleDataset(uuid))
        result = edit_resource(current)
        client.request(UpdateDataset(uuid, payload=result))
        echo("Succesfully updated dataset")
