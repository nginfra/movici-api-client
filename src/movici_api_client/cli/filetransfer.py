import contextlib
import json
import pathlib

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from movici_api_client.api import Client
from movici_api_client.api.common import Request
from movici_api_client.api.requests import (
    AddDatasetData,
    CreateDataset,
    GetDatasetData,
    GetDatasets,
    GetDatasetTypes,
    ModifiyDatasetData,
)
from movici_api_client.cli.exceptions import InvalidFile

from . import dependencies
from .utils import confirm, echo, prompt_choices


@contextlib.contextmanager
def monitored_file(file: pathlib.Path):
    with open(file, "rb") as fobj, tqdm(
        total=file.stat().st_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file.name
    ) as t:
        yield CallbackIOWrapper(t.update, fobj, "read")
        t.reset()


def upload_new_dataset(uuid, file: pathlib.Path):
    client = dependencies.get(Client)
    with monitored_file(file) as fobj:
        return client.request(AddDatasetData(uuid, fobj))


def upload_existing_dataset(uuid, file):
    client = dependencies.get(Client)
    with monitored_file(file) as fobj:
        return client.request(ModifiyDatasetData(uuid, fobj))


def upload_multiple(
    directory,
    project_uuid,
    extensions=None,
    overwrite=None,
    create_new=None,
    inspect_files=False,
):
    uploader = MultipleDatasetUploader(directory, project_uuid, extensions=extensions)
    return uploader.upload(overwrite, create_new, inspect_files)


class MultipleDatasetUploader:
    def __init__(self, directory, project_uuid, client: Client = None, extensions=None):
        self.directory = directory
        self.project_uuid = project_uuid
        self.client = client or dependencies.get(Client)
        self.extensions = extensions
        self.all_dataset_types = None

    def upload(self, overwrite, create_new, inspect_files):
        client = self.client
        all_datasets = client.request(GetDatasets(self.project_uuid))
        dataset_with_data = set(d["name"] for d in all_datasets if d["has_data"])
        dataset_uuids = {d["name"]: d["uuid"] for d in all_datasets}

        for file in tqdm(list(self.iter_dataset_files()), desc="Processing files"):
            name = file.stem
            if name not in dataset_uuids:
                if not self.maybe_create_new_dataset(create_new, name):
                    continue
                self.ensure_all_dataset_types()
                dataset_type = self.infer_dataset_type(file, inspect_file=inspect_files)
                uuid = client.request(
                    CreateDataset(self.project_uuid, name, type=dataset_type, display_name=name)
                )["dataset_uuid"]
                has_data = False
            else:
                uuid = dataset_uuids[name]
                has_data = name in dataset_with_data

            if has_data:
                if not self.maybe_overwrite_data(overwrite, name):
                    continue
                upload_existing_dataset(uuid, file)
            else:
                upload_new_dataset(uuid, file)

    def iter_dataset_files(self):
        for file in self.directory.glob("*"):
            if not file.is_file():
                continue
            if self.extensions is None or file.suffix in self.extensions:
                yield file

    def ensure_all_dataset_types(self):
        if self.all_dataset_types is None:
            self.all_dataset_types = self.client.request(GetDatasetTypes())

    @staticmethod
    def maybe_create_new_dataset(create_new, name):
        do_create = maybe_set_flag(
            create_new, f"Dataset {name} does not exist, do you wish to create this dataset?"
        )
        if not do_create:
            echo(f"Cowardly refusing to create new dataset {name}")
        return do_create

    @staticmethod
    def maybe_overwrite_data(overwrite, name):
        do_overwrite = maybe_set_flag(
            overwrite, f"Dataset {name} already has data, do you wish to overwrite?"
        )
        if not do_overwrite:
            echo(f"Cowardly refusing to overwrite data for '{name}'")
        return do_overwrite

    def infer_dataset_type(self, file: pathlib.Path, inspect_file=True):
        if file.suffix in (".csv"):
            return "parameters"
        if file.suffix in (".nc"):
            return "flooding_tape"
        if file.suffix in (".tiff", ".tif", ".geotif", ".geotif"):
            return "height_map"
        if inspect_file:
            if file.suffix == ".json":
                contents = json.loads(file.read_bytes())
                try:
                    return contents["type"]
                except KeyError:
                    pass
        if not self.all_dataset_types:
            echo(f"Could not determine dataset type for '{file.name}'")
            return
        return prompt_choices(
            f"Please specify the type for dataset '{file.name}'", self.all_dataset_types
        )


def maybe_set_flag(flag, confirm_message):
    result = flag
    if flag is None:
        result = confirm(confirm_message)
    return result


def download_dataset_data(
    client: Client, name: str, uuid: str, directory: pathlib.Path, overwrite=None
):
    target = directory.joinpath(name)
    download_as_file(client, GetDatasetData(uuid), file=target, overwrite=overwrite)


def download_multiple_dataset_data(
    client: Client, project_uuid: str, directory: pathlib.Path, overwrite=None
):
    all_datasets = client.request(GetDatasets(project_uuid))
    for dataset in tqdm(all_datasets, desc="Downloading files"):
        name, uuid = dataset["name"], dataset["uuid"]
        download_dataset_data(
            client, name=name, uuid=uuid, directory=directory, overwrite=overwrite
        )


def download_as_file(
    client: Client, request: Request, file: pathlib.Path, infer_extension=True, overwrite=None
):
    with client.stream(request) as response:
        if infer_extension:
            file = file.with_suffix(EXTENSIONS.get(response.headers.get("content-type"), ".dat"))
        if file.exists():
            overwrite = maybe_set_flag(overwrite, "File already existing, overwrite?")
            if not file.is_file():
                raise InvalidFile(file)
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
