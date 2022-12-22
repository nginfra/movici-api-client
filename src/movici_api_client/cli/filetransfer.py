from __future__ import annotations

import contextlib
import pathlib
import typing as t

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from movici_api_client.api import Client
from movici_api_client.api.common import Request
from movici_api_client.api.requests import (
    AddDatasetData,
    CreateDataset,
    CreateScenario,
    GetDatasets,
    GetDatasetTypes,
    GetScenarios,
    ModifiyDatasetData,
    UpdateScenario,
)
from movici_api_client.cli.exceptions import InvalidFile

from . import dependencies
from .helpers import read_json_file
from .utils import confirm, echo, prompt_choices


def download_resource(
    client: Client, name: str, request: Request, directory: pathlib.Path, overwrite=None
):
    target = directory.joinpath(name)
    download_as_file(client, request, file=target, overwrite=overwrite)


def download_multiple_dataset_data(
    client: Client, project_uuid: str, directory: pathlib.Path, overwrite=None
):
    all_datasets = client.request(GetDatasets(project_uuid))
    for dataset in tqdm(all_datasets, desc="Downloading files"):
        name, uuid = dataset["name"], dataset["uuid"]
        download_resource(client, name=name, uuid=uuid, directory=directory, overwrite=overwrite)


def download_as_file(
    client: Client, request: Request, file: pathlib.Path, infer_extension=True, overwrite=None
):
    with client.stream(request) as response:
        if infer_extension:
            file = file.with_suffix(EXTENSIONS.get(response.headers.get("content-type"), ".dat"))
        if file.exists():
            overwrite = resolve_question_flag(overwrite, "File already existing, overwrite?")
            if not file.is_file():
                raise InvalidFile(msg="not a file", file=file)
            if not overwrite:
                echo(f"Cowardly refusing to overwrite existing file {file.name}")
                return
        with tqdm.wrapattr(
            open(file, "wb"),
            "write",
            miniters=1,
            desc=file.name,
            total=int(response.headers.get("content-length", 0)),
        ) as fout:
            for chunk in response.iter_bytes(chunk_size=4096):
                fout.write(chunk)


EXTENSIONS = {
    "application/json": ".json",
    "application/msgpack": ".msgpack",
    "application/x-msgpack": ".msgpack",
    "application/netcdf": ".nc",
    "application/x-netcdf": ".nc",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/plain": ".txt",
    "image/tiff": ".tif",
}


class ResourceUploader:
    def __init__(
        self,
        file: pathlib.Path,
        parent_uuid: str,
        strategy: UploadStrategy,
        all_resources: t.Sequence[dict] = None,
    ):
        self.file = file
        self.parent_uuid = parent_uuid
        self.strategy = strategy
        self.all_resources = all_resources

    def upload(self, overwrite, create_new, inspect_file, name=None):
        self.ensure_all_resources()
        resources_by_name = {r["name"]: r for r in self.all_resources}

        name = name or self.file.stem
        if not (existing := resources_by_name.get(name)):
            if self.determine_create_new(create_new, name):
                return self.strategy.create_new(
                    self.parent_uuid, file=self.file, name=name, inspect_file=inspect_file
                )

        if self.strategy.require_overwrite_question(existing) and not self.determine_overwrite(
            overwrite, name
        ):
            return

        self.strategy.update_existing(existing, self.file, inspect_file)

    def ensure_all_resources(self):
        if self.all_resources is None:
            self.all_resources = self.strategy.get_all()

    def determine_create_new(self, create_new, name):
        resource_type = self.strategy.resource_type
        do_create = resolve_question_flag(
            create_new,
            (
                f"{resource_type.capitalize()} {name} does not exist, "
                f"do you wish to create this {resource_type}?",
            ),
        )
        if not do_create:
            echo(f"Cowardly refusing to create new {resource_type} {name}")
        return do_create

    def determine_overwrite(self, overwrite, name):
        resource_type = self.strategy.resource_type

        do_overwrite = resolve_question_flag(
            overwrite,
            (
                f"{resource_type.capitalize()} {name} already has data, "
                "do you wish to overwrite?"
            ),
        )
        if not do_overwrite:
            echo(f"Cowardly refusing to overwrite data for {resource_type} '{name}'")
        return do_overwrite


class MultipleResourceUploader:
    def __init__(
        self,
        directory,
        parent_uuid: str,
        strategy: UploadStrategy,
        uploader_cls=ResourceUploader,
    ):
        self.directory = directory
        self.parent_uuid = parent_uuid
        self.strategy = strategy
        self.uploader_cls = uploader_cls

    def upload(self, overwrite, create_new, inspect_files):
        all_resources = self.strategy.get_all(self.parent_uuid)

        for file in tqdm(
            list(self.strategy.iter_files(self.directory)),
            desc=f"Processing {self.strategy.resource_type} files",
        ):
            uploader = self.uploader_cls(
                file,
                self.parent_uuid,
                strategy=self.strategy,
                all_resources=all_resources,
            )
            uploader.upload(overwrite=overwrite, create_new=create_new, inspect_file=inspect_files)


@contextlib.contextmanager
def read_file_progress_bar(file: pathlib.Path):
    with open(file, "rb") as fobj, tqdm(
        total=file.stat().st_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file.name
    ) as t:
        yield CallbackIOWrapper(t.update, fobj, "read")
        t.reset()


def resolve_question_flag(flag, confirm_message):
    result = flag
    if flag is None:
        result = confirm(confirm_message)
    return result


class UploadStrategy:
    extensions: t.Optional[t.Collection]
    messages: dict
    resource_type: str

    def iter_files(self, directory: pathlib.Path):
        for file in directory.glob("*"):
            if not file.is_file():
                continue
            if self.extensions is None or file.suffix in self.extensions:
                yield file

    def __init__(self, client: Client = None):
        self.client = client or dependencies.get(Client)

    def get_all(self):
        raise NotImplementedError

    def require_overwrite_question(self, existing: dict):
        return True

    def create_new(self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False):
        raise NotImplementedError

    def update_existing(self, existing: dict, file: pathlib.Path, name=None, inspect_file=False):
        raise NotADirectoryError


class DatasetUploadStrategy(UploadStrategy):
    extensions = {".json", ".msgpack", ".csv", ".nc", ".tiff", ".tif", ".geotif", ".geotif"}
    messages = {"processing": "Processing files"}
    resource_type = "dataset"

    def __init__(self, client: Client = None):
        super().__init__(client)
        self.all_dataset_types = None

    def get_all(self, parent_uuid: str):
        return self.client.request(GetDatasets(project_uuid=parent_uuid))

    def require_overwrite_question(self, existing: dict):
        return existing["has_data"]

    def create_new(self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False):
        name = name or file.stem
        self.ensure_all_dataset_types()
        dataset_type = self.infer_dataset_type(file, inspect_file=inspect_file)
        uuid = self.client.request(
            CreateDataset(parent_uuid, name, type=dataset_type, display_name=name)
        )["dataset_uuid"]
        self.upload_new_data(uuid, file)

    def update_existing(self, existing: dict, file: pathlib.Path, inspect_file=False):
        uuid, has_data = existing["uuid"], existing["has_data"]

        if has_data:
            self.upload_existing_data(uuid, file)
        else:
            self.upload_new_data(uuid, file)

    def ensure_all_dataset_types(self):
        if self.all_dataset_types is None:
            self.all_dataset_types = self.client.request(GetDatasetTypes())

    def infer_dataset_type(self, file: pathlib.Path, inspect_file):
        if file.suffix in (".csv"):
            return "parameters"
        if file.suffix in (".nc"):
            return "flooding_tape"
        if file.suffix in (".tiff", ".tif", ".geotif", ".geotif"):
            return "height_map"
        if inspect_file:
            if file.suffix == ".json":
                try:
                    return read_json_file(file)["type"]
                except (InvalidFile, KeyError):
                    pass
        if not self.all_dataset_types:
            echo(f"Could not determine dataset type for '{file.name}'")
            return
        return prompt_choices(
            f"Please specify the type for dataset '{file.name}'", self.all_dataset_types
        )

    def upload_new_data(self, uuid, file):
        with read_file_progress_bar(file) as fobj:
            return self.client.request(AddDatasetData(uuid, fobj))

    def upload_existing_data(self, uuid, file):
        with read_file_progress_bar(file) as fobj:
            return self.client.request(ModifiyDatasetData(uuid, fobj))


class ScenarioUploadStrategy(UploadStrategy):
    extensions = {".json"}

    def get_all(self, parent_uuid: str):
        return self.client.request(GetScenarios(project_uuid=parent_uuid))

    def create_new(self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False):
        payload = self._prepare_payload(name or file.stem, file, inspect_file)
        return self.client.request(CreateScenario(parent_uuid, payload))

    def update_existing(self, existing: dict, file: pathlib.Path, name=None, inspect_file=False):
        payload = self._prepare_payload(name or file.stem, file, inspect_file)
        return self.client.request(UpdateScenario(existing["uuid"], payload))

    def _prepare_payload(self, name, file, inspect_file):
        payload = read_json_file(file)
        if inspect_file:
            payload["name"] = name
        return payload
