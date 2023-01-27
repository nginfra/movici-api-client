import itertools
import json
import pathlib
import typing as t

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

from ..common import CLIParameters
from ..cqrs import Event, EventHandler, Mediator
from ..data_dir import MoviciDataDir
from ..events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    EditProject,
    GetAllProjects,
    GetSingleProject,
    UploadProject,
)
from ..exceptions import Conflict, InvalidUsage, NoChangeDetected, NotFound
from ..utils import confirm  # noqa TODO: remove noqa once confirm is used


class LocalEventHandler(EventHandler):
    def __init__(self, params: CLIParameters, directory: MoviciDataDir) -> None:
        self.params = params
        self.directory = directory


class UnsupportedEventsHandler(LocalEventHandler):
    __event__ = (
        GetAllProjects,
        GetSingleProject,
        CreateProject,
        UploadProject,
        DeleteProject,
        UploadProject,
        DownloadProject,
        EditProject,
        ClearDataset,
        UploadDataset,
        UploadMultipleDatasets,
        DownloadDataset,
        DownloadMultipleDatasets,
        EditDataset,
    )

    async def handle(self, event: Event, mediator: Mediator):
        raise InvalidUsage("Local contexts do no support projects")


class LocalDatasetsHandler(LocalEventHandler):
    desired_keys = {"uuid", "display_name", "type", "format"}

    async def handle(self, event: Event, mediator: Mediator):
        raise InvalidUsage("This command is not supported for local contexts")

    def get_dataset_meta(self, file: pathlib.Path):
        ds = {"name": file.stem}
        if self.params.inspect and file.suffix == ".json":
            ds.update(self.inspect_json_file(file))
        if "format" not in ds:
            ds_type, ds_format = self.get_type_and_format(file, ds.get("type"))
            if ds_type is not None:
                ds["type"] = ds_type
            if ds_format is not None:
                ds["format"] = ds_format
        if ds.get("format") == "binary":
            ds["has_data"] = True
        return ds

    def inspect_json_file(self, file: pathlib.Path):
        full_dataset = json.loads(file.read_bytes())
        result = {}
        for key in full_dataset.keys() & self.desired_keys:
            result[key] = full_dataset[key]
        result["has_data"] = "data" in full_dataset
        return result

    @staticmethod
    def get_type_and_format(file: pathlib.Path, dataset_type):
        if dataset_type is None:
            if file.suffix == ".nc":
                dataset_type = "flooding_tape"
            elif file.suffix in (".tif", ".tiff", ".geotiff", ".geotif"):
                dataset_type = "height_map"
        formats = {
            "tabular": "unstructured",
            "height_map": "binary",
            "flooding_tape": "binary",
            None: None,
        }
        return dataset_type, formats.get(dataset_type, "entity_based")

    def get_file_raise_on_not_found(self, name):
        file = self.directory.datasets().get_file_path_if_exists(name)
        if file is None:
            raise NotFound("Dataset", name)
        return file

    def get_file_raise_on_conflict(self, name, suffix):
        file = self.directory.datasets().get_file_path_if_exists(name)
        if file is not None:
            raise Conflict("Dataset", name)
        return self.directory.datasets().get_file_path(name + suffix)


class LocalGetAllDatasetsHandler(LocalDatasetsHandler):
    __event__ = GetAllDatasets

    async def handle(self, event: Event, mediator: Mediator):
        return [self.get_dataset_meta(file) for file in self.directory.datasets().iter_files()]


class LocalGetSingleHandler(LocalDatasetsHandler):
    __event__ = GetSingleDataset

    async def handle(self, event: GetSingleDataset, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid)
        return self.get_dataset_meta(file)


class LocalCreateDatasetHandler(LocalDatasetsHandler):
    __event__ = CreateDataset

    async def handle(self, event: CreateDataset, mediator: Mediator):
        file = self.get_file_raise_on_conflict(event.name, ".json")

        payload = {"name": event.name, "display_name": event.display_name}
        if event.type is not None:
            payload["type"] = event.type

        file.write_text(json.dumps(payload))

        return "Dataset succesfully created"


class LocalUpdateDatasetHandler(LocalDatasetsHandler):
    __event__ = UpdateDataset

    async def handle(self, event: UpdateDataset, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid)
        payload = event.payload()
        if not payload:
            raise NoChangeDetected()
        if event.name and event.name != event.name_or_uuid:
            new_file = self.get_file_raise_on_conflict(event.name, file.suffix)
            file.rename(new_file)
            file = new_file

        if file.suffix == ".json":
            contents = json.loads(file.read_bytes())
            contents.update(payload)
            file.write_text(json.dumps(contents))
        return "Dataset succesfully updated"


class LocalDeleteDatasetHandler(LocalDatasetsHandler):
    __event__ = DeleteDataset

    async def handle(self, event: Event, mediator: Mediator):
        return await super().handle(event, mediator)


def parse_handler(handler: t.Type[EventHandler]):
    if handler.__event__ is None:
        return
    if isinstance(handler.__event__, (tuple, list)):
        yield from ((e, handler) for e in handler.__event__)
    else:
        yield (handler.__event__, handler)


ALL_HANDLERS = dict(
    itertools.chain.from_iterable(
        parse_handler(obj)
        for obj in globals().values()
        if isinstance(obj, type) and obj is not EventHandler and issubclass(obj, EventHandler)
    )
)
