import dataclasses
import pathlib

from movici_api_client.cli.data_dir import DataDir

from ..cqrs import Event


@dataclasses.dataclass
class GetAllScenarios(Event):
    pass


@dataclasses.dataclass
class GetSingleScenario(Event):
    name_or_uuid: str


@dataclasses.dataclass
class CreateScenario(Event):
    name: str
    payload: dict


@dataclasses.dataclass
class DeleteScenario(Event):
    name_or_uuid: str


@dataclasses.dataclass
class ClearScenario(Event):
    name_or_uuid: str
    confirm: bool


@dataclasses.dataclass
class RunSimulation(Event):
    name_or_uuid: str


@dataclasses.dataclass
class UploadScenario(Event):
    name_or_uuid: str
    file: pathlib.Path


@dataclasses.dataclass
class UploadMultipleScenarios(Event):
    directory: DataDir


@dataclasses.dataclass
class DownloadScenario(Event):
    name_or_uuid: str
    directory: DataDir


@dataclasses.dataclass
class DownloadMultipleScenarios(Event):
    directory: DataDir


@dataclasses.dataclass
class EditScenario(Event):
    name_or_uuid: str
