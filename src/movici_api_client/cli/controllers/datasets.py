import functools
import pathlib
import typing as t

from movici_api_client.api.client import Client
from movici_api_client.api.requests import (
    CreateDataset,
    DeleteDataset,
    DeleteDatasetData,
    GetDatasets,
    GetDatasetTypes,
    GetSingleDataset,
    UpdateDataset,
)
from movici_api_client.cli.dependencies import get
from movici_api_client.cli.exceptions import InvalidResource, InvalidUsage, NotYetImplemented
from movici_api_client.cli.filetransfer import (
    download_dataset_data,
    download_multiple_dataset_data,
    upload_multiple,
    upload_new_dataset,
)
from movici_api_client.cli.utils import DirPath, confirm, echo, prompt

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    command,
    format_output,
    option,
    valid_project_uuid,
)
from ..utils import (
    FilePath,
    assert_resource_uuid,
    prompt_choices,
    resolve_question_flag,
    validate_uuid,
)


def combine_decorators(decorators: t.Iterable[callable]):
    def decorator(func):
        return functools.reduce(
            lambda f, decorator: decorator(f),
            decorators,
            func,
        )

    return decorator


def upload_data_options(func):
    return combine_decorators(
        [
            option("-o", "--overwrite", is_flag=True, help="Always overwrite existing data"),
            option("-c", "--create", is_flag=True, help="Always create new datasets"),
            option(
                "-y",
                "--yes",
                is_flag=True,
                help="Answer yes to all questions, equivalent to -o -c",
            ),
            option("-n", "--no", is_flag=True, help="Answer no to all questions"),
            option(
                "-i",
                "--inspect",
                is_flag=True,
                help="Try to read files to determine their dataset type, less performant",
            ),
        ]
    )(func)


def download_data_options(func):
    return combine_decorators(
        [
            option("-d", "--directory", type=DirPath(writable=True), default=pathlib.Path(".")),
            option("-o", "-y", "--overwrite", is_flag=True, help="Always overwrite existing data"),
            option("-n", "--no-overwrite", is_flag=True, help="Never overwrite existing data"),
        ]
    )(func)


class DatasetCrontroller(Controller):
    name = "dataset"
    decorators = (authenticated, valid_project_uuid)

    @command(name="datasets")
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
    @upload_data_options
    def upload(
        self, project_uuid, name_or_uuid, file: pathlib.Path, overwrite, create, yes, no, inspect
    ):
        name_or_uuid = name_or_uuid or file.stem
        client = get(Client)
        uuid = get_dataset_uuid(name_or_uuid, project_uuid, client=client)
        return upload_new_dataset(uuid, file)

    @command(name="datasets", group="upload")
    @option("-d", "--directory", type=DirPath(), required=True)
    @upload_data_options
    def upload_multiple(self, project_uuid, directory, overwrite, create, yes, no, inspect):
        if yes and no:
            raise InvalidUsage("cannot combine --force with --never")

        upload_multiple(
            directory,
            project_uuid,
            extensions={".json", ".msgpack", ".csv", ".nc", ".tiff", ".tif", ".geotif", ".geotif"},
            overwrite=resolve_question_flag(overwrite, yes, no),
            create_new=resolve_question_flag(create, yes, no),
            inspect_files=inspect,
        )
        echo("Success!")

    @command
    @argument("name_or_uuid")
    @download_data_options
    def download(self, project_uuid, name_or_uuid, directory, overwrite, no_overwrite):
        client = get(Client)
        name, uuid = get_dataset_name_and_uuid(name_or_uuid, project_uuid)
        download_dataset_data(
            client=client,
            name=name,
            uuid=uuid,
            directory=directory,
            overwrite=resolve_question_flag(False, default_yes=overwrite, default_no=no_overwrite),
        )

    @command(name="datasets", group="download")
    @download_data_options
    def download_multiple(self, project_uuid, directory, overwrite, no_overwrite):
        client = get(Client)
        download_multiple_dataset_data(
            client=client,
            project_uuid=project_uuid,
            directory=directory,
            overwrite=resolve_question_flag(False, default_yes=overwrite, default_no=no_overwrite),
        )


def get_dataset_uuid(name_or_uuid, project_uuid, client=None):
    client = client or get(Client)
    return (
        name_or_uuid
        if validate_uuid(name_or_uuid)
        else assert_resource_uuid(name_or_uuid, GetDatasets(project_uuid), "dataset")
    )


def get_dataset_name_and_uuid(name_or_uuid, project_uuid, client=None):
    client = client or get(Client)
    all_datasets = client.request(GetDatasets(project_uuid))

    try:
        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
            name = next(d["name"] for d in all_datasets if d["uuid"] == uuid)
        else:
            name = name_or_uuid
            uuid = next(d["uuid"] for d in all_datasets if d["name"] == name)
    except StopIteration:
        raise InvalidResource("dataset", name_or_uuid)
    return name, uuid
