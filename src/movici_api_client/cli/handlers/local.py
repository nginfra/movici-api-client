import itertools
import json
import pathlib
import typing as t

from movici_api_client.cli.helpers import edit_resource

from ..common import CLIParameters
from ..cqrs import Event, EventHandler, Mediator
from ..data_dir import MoviciDataDir, SimpleDataDirectory
from ..events.dataset import (
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
from ..events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    EditProject,
    GetAllProjects,
    GetSingleProject,
    UploadProject,
)
from ..events.scenario import (
    ClearScenario,
    CreateScenario,
    DeleteScenario,
    DownloadMultipleScenarios,
    DownloadScenario,
    EditScenario,
    GetAllScenarios,
    GetSingleScenario,
    RunSimulation,
    UploadMultipleScenarios,
    UploadScenario,
)
from ..exceptions import Conflict, InvalidUsage, NoChangeDetected, NotFound
from ..utils import confirm


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
        DownloadMultipleScenarios,
        UploadScenario,
        UploadMultipleScenarios,
        DownloadScenario,
        RunSimulation,
    )

    async def handle(self, event: Event, mediator: Mediator):
        raise InvalidUsage("This command is not supported for local contexts")


class LocalResourceCRUDHandler(LocalEventHandler):
    __subhandlers__ = None
    resource_type = "resource"

    def __init_subclass__(cls) -> None:
        if isinstance(cls.__subhandlers__, dict):
            cls.__event__ = tuple(cls.__subhandlers__.keys())

    async def handle(self, event: Event, mediator: Mediator):
        try:
            handler = getattr(self, self.__subhandlers__[type(event)])
        except (KeyError, AttributeError):
            raise RuntimeError(f"Handler mismatch for event {type(event).__name__}")
        return await handler(event, mediator)

    async def handle_get_all(self, event: Event, mediator: Mediator):
        return [self.get_resource_meta(file) for file in self.data_dir(event).iter_files()]

    async def handle_get_single(self, event: Event, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid, event=Event)
        return self.get_resource_meta(file)

    async def handle_create(self, event: Event, mediator: Mediator):
        file = self.get_file_raise_on_conflict(event.name, ".json", event=Event)
        file.write_text(json.dumps(event.payload()))
        return f"{self.resource_type.capitalize()} succesfully created"

    async def handle_update(self, event: Event, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid, event=Event)
        payload = event.payload()
        if not payload:
            raise NoChangeDetected()
        if event.name and event.name != event.name_or_uuid:
            new_file = self.get_file_raise_on_conflict(event.name, file.suffix, event=Event)
            file.rename(new_file)
            file = new_file

        if file.suffix == ".json":
            contents = json.loads(file.read_bytes())
            contents.update(payload)
            file.write_text(json.dumps(contents))
        return f"{self.resource_type.capitalize()} succesfully updated"

    async def handle_delete(self, event: DeleteDataset, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid, event=Event)
        confirm(f"Are you sure you wish to delete {self.resource_type} '{event.name_or_uuid}'")
        file.unlink()
        return f"{self.resource_type.capitalize()} succesfully deleted"

    async def handle_edit(self, event: EditDataset, mediator: Mediator):
        file = self.get_file_raise_on_not_found(event.name_or_uuid, event=Event)
        if file.suffix != ".json":
            raise InvalidUsage("Can only edit json files")
        current = json.loads(file.read_bytes())
        result = edit_resource(current)
        file.write_text(json.dumps(result, indent=2))

    def get_file_raise_on_not_found(self, name, event: Event):
        file = self.data_dir(event).get_file_path_if_exists(name)
        if file is None:
            raise NotFound(self.resource_type.capitalize(), name)
        return file

    def get_file_raise_on_conflict(self, name, suffix, event: Event):
        file = self.data_dir(event).get_file_path_if_exists(name)
        if file is not None:
            raise Conflict(self.resource_type.capitalize(), name)
        return self.data_dir(event).get_file_path(name + suffix)

    def data_dir(self, event: Event) -> SimpleDataDirectory:
        raise NotImplementedError

    def get_resource_meta(self, file: pathlib.Path):
        raise NotImplementedError


class LocalDatasetsHandler(LocalResourceCRUDHandler):
    resource_type = "dataset"
    inspect_keys = {"uuid", "display_name", "type", "format"}
    __subhandlers__ = {
        GetAllDatasets: "handle_get_all",
        GetSingleDataset: "handle_get_single",
        CreateDataset: "handle_create",
        UpdateDataset: "handle_update",
        DeleteDataset: "handle_delete",
        EditDataset: "handle_edit",
    }

    def data_dir(self, event: Event):
        return self.directory.datasets()

    def get_resource_meta(self, file: pathlib.Path):
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
        for key in full_dataset.keys() & self.inspect_keys:
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


class LocalScenariosHandler(LocalResourceCRUDHandler):
    resource_type = "dataset"
    inspect_keys = {
        "uuid",
        "name",
        "display_name",
        "description",
        "bounding_box",
        "epsg_code",
        "created_on",
        "last_modified",
        "status",
        "has_timeline",
    }
    __subhandlers__ = {
        GetAllScenarios: "handle_get_all",
        GetSingleScenario: "handle_get_single",
        CreateScenario: "handle_create",
        DeleteScenario: "handle_delete",
        EditScenario: "handle_edit",
    }

    def data_dir(self, event: Event):
        return self.directory.scenarios()

    def get_resource_meta(self, file: pathlib.Path):
        ds = {"name": file.stem}
        if self.params.inspect and file.suffix == ".json":
            ds.update(self.inspect_json_file(file))
        return ds

    def inspect_json_file(self, file: pathlib.Path):
        contents = json.loads(file.read_bytes())
        result = {}
        for key in contents.keys() & self.inspect_keys:
            result[key] = contents[key]
        return result


class LocalClearScenarioHandler(LocalEventHandler):
    __event__ = ClearScenario


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
