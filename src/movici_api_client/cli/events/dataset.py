import dataclasses
import pathlib
import typing as t

from movici_api_client.cli.data_dir import DataDir

from ..cqrs import Event


@dataclasses.dataclass
class GetAllDatasets(Event):
    pass


@dataclasses.dataclass
class GetDatasetTypes(Event):
    pass


@dataclasses.dataclass
class GetSingleDataset(Event):
    name_or_uuid: str


@dataclasses.dataclass
class CreateDataset(Event):
    name: str
    display_name: str
    type: t.Optional[str] = None


@dataclasses.dataclass
class UpdateDataset(Event):
    name_or_uuid: str
    name: t.Optional[str] = None
    display_name: t.Optional[str] = None
    type: t.Optional[str] = None

    def payload(self):
        return {
            key: val for key in ("name", "display_name", "type") if (val := getattr(self, key))
        }


@dataclasses.dataclass
class DeleteDataset(Event):
    name_or_uuid: str


@dataclasses.dataclass
class ClearDataset(Event):
    name_or_uuid: str


@dataclasses.dataclass
class UploadDataset(Event):
    name_or_uuid: str
    file: pathlib.Path


@dataclasses.dataclass
class UploadMultipleDatasets(Event):
    directory: DataDir


@dataclasses.dataclass
class DownloadDataset(Event):
    name_or_uuid: str
    directory: DataDir


@dataclasses.dataclass
class DownloadMultipleDatasets(Event):
    directory: DataDir


@dataclasses.dataclass
class EditDataset(Event):
    name_or_uuid: str
