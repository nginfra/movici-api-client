import dataclasses

from movici_api_client.cli.data_dir import MoviciDataDir

from ..cqrs import Event


@dataclasses.dataclass
class GetAllProjects(Event):
    pass


@dataclasses.dataclass
class GetSingleProject(Event):
    name_or_uuid: str


@dataclasses.dataclass
class CreateProject(Event):
    name: str
    display_name: str


@dataclasses.dataclass
class UpdateProject(Event):
    name_or_uuid: str
    display_name: str


@dataclasses.dataclass
class DeleteProject(Event):
    name_or_uuid: str


@dataclasses.dataclass
class UploadProject(Event):
    directory: MoviciDataDir


@dataclasses.dataclass
class DownloadProject(Event):
    directory: MoviciDataDir


@dataclasses.dataclass
class EditProject(Event):
    name_or_uuid: str
