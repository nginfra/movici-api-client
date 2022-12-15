import contextlib
import json
import pathlib
import typing as t

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from movici_api_client.api import Client
from movici_api_client.api.requests import (
    AddDatasetData,
    CreateDataset,
    GetDatasetTypes,
    GetDatasets,
    ModifiyDatasetData,
)

from . import dependencies
from .utils import confirm, echo, prompt_choices


@contextlib.contextmanager
def monitored_file(file: pathlib.Path):
    with (
        open(file, "rb") as fobj,
        tqdm(
            total=file.stat().st_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file.name
        ) as t,
    ):
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
        all_types = None

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
        do_create = create_new
        if create_new is None:
            do_create = confirm(
                f"Dataset {name} does not exist, do you wish to create this dataset?"
            )
        if not do_create:
            echo(f"Cowardly refusing to create new dataset {name}")
        return do_create

    @staticmethod
    def maybe_overwrite_data(overwrite, name):
        do_overwrite = overwrite
        if overwrite is None:
            do_overwrite = confirm(f"Dataset {name} already has data, do you wish to overwrite?")
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