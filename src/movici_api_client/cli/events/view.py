import dataclasses
import pathlib

from movici_api_client.cli.data_dir import DataDir

from ..cqrs import Event


@dataclasses.dataclass
class GetAllViews(Event):
    scenario_name_or_uuid: str


@dataclasses.dataclass
class GetSingleView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str


@dataclasses.dataclass
class DeleteView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str


@dataclasses.dataclass
class UploadView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str
    file: pathlib.Path


@dataclasses.dataclass
class UploadMultipleViews(Event):
    scenario_name_or_uuid: str
    directory: DataDir


@dataclasses.dataclass
class DownloadView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str
    directory: DataDir


@dataclasses.dataclass
class DownloadMultipleViews(Event):
    scenario_name_or_uuid: str
    directory: DataDir


@dataclasses.dataclass
class EditView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str


@dataclasses.dataclass
class DuplicateView(Event):
    scenario_name_or_uuid: str
    view_name_or_uuid: str
    new_view_name: str
