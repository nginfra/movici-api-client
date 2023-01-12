import dataclasses
import io
import pathlib
import typing as t

import httpx

from .common import Request, Service, pick, simple_request, unwrap_envelope, urljoin


class AuthRequest(Request):
    service = Service.AUTH


class DataEngineRequest(Request):
    service = Service.DATA_ENGINE


class SimulationControlRequest(Request):
    service = Service.MODEL_ENGINE


@dataclasses.dataclass
class Login(AuthRequest):
    auth = False
    username: str
    password: str

    def make_request(self):
        return {
            "method": "POST",
            "url": "user/login",
            "json": pick(self, ["username", "password"]),
        }


@dataclasses.dataclass
class CheckAuthToken(AuthRequest):
    service = Service.AUTH

    auth_token: t.Optional[str] = None

    def make_request(self):
        req = {
            "method": "GET",
            "url": "auth",
        }
        if self.auth_token is not None:
            req["headers"] = {"Authorization": self.auth_token}
        return req


@unwrap_envelope("projects")
class GetProjects(DataEngineRequest):
    @simple_request
    def make_request(self):
        return "projects"


@dataclasses.dataclass
class GetSingleProject(DataEngineRequest):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin("projects", self.uuid)


@dataclasses.dataclass
class CreateProject(DataEngineRequest):

    name: str
    display_name: str

    def make_request(self):
        return {
            "method": "POST",
            "url": "projects",
            "json": pick(self, ["name", "display_name"]),
        }


@dataclasses.dataclass
class UpdateProject(DataEngineRequest):
    uuid: str
    display_name: str

    def make_request(self):
        return {
            "method": "PUT",
            "url": urljoin("projects", self.uuid),
            "json": pick(self, ["display_name"]),
        }


@dataclasses.dataclass
class DeleteProject(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin("projects", self.uuid),
        }


@dataclasses.dataclass
@unwrap_envelope("datasets")
class GetDatasets(DataEngineRequest):
    project_uuid: str

    @simple_request
    def make_request(self):
        return urljoin("projects", self.project_uuid, "datasets")


@dataclasses.dataclass
class GetSingleDataset(DataEngineRequest):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin("datasets", self.uuid)


@dataclasses.dataclass
class CreateDataset(DataEngineRequest):
    project_uuid: str
    name: str
    type: str
    display_name: t.Optional[str] = None

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin("projects", self.project_uuid, "datasets"),
            "json": pick(self, ("name", "type", "display_name")),
        }


@dataclasses.dataclass
class UpdateDataset(DataEngineRequest):
    uuid: str
    name: t.Optional[str] = None
    type: t.Optional[str] = None
    display_name: t.Optional[str] = None

    def make_request(self):
        return {
            "method": "PUT",
            "url": urljoin("datasets", self.uuid),
            "json": {
                k: v
                for k, v in pick(self, ("name", "type", "display_name")).items()
                if v is not None
            },
        }


@dataclasses.dataclass
class DeleteDataset(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {"method": "DELETE", "url": urljoin("datasets", self.uuid)}


@dataclasses.dataclass
class GetDatasetData(DataEngineRequest):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin("datasets", self.uuid, "data")


@dataclasses.dataclass
class AddDatasetData(DataEngineRequest):
    uuid: str
    file: t.Union[str, pathlib.Path, io.BufferedIOBase]

    def make_request(self):
        file = self.file
        if isinstance(file, (str, pathlib.Path)):
            file = open(file, "rb")

        return {
            "method": "POST",
            "url": urljoin("datasets", self.uuid, "data"),
            "files": {
                "data": file,
            },
            "timeout": httpx.Timeout(10.0, read=60.0),
        }


class ModifiyDatasetData(AddDatasetData):
    def make_request(self):
        result = {**super().make_request(), "params": {"overwrite": True}}
        return result


@dataclasses.dataclass
class DeleteDatasetData(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin("datasets", self.uuid, "data"),
        }


@dataclasses.dataclass
@unwrap_envelope("scenarios")
class GetScenarios(DataEngineRequest):
    project_uuid: str

    @simple_request
    def make_request(self):
        return urljoin("projects", self.project_uuid, "scenarios")


@dataclasses.dataclass
class GetSingleScenario(DataEngineRequest):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin("scenarios", self.uuid)


@dataclasses.dataclass
class CreateScenario(DataEngineRequest):
    project_uuid: str
    payload: dict

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin("projects", self.project_uuid, "scenarios"),
            "json": self.payload,
        }


@dataclasses.dataclass
class UpdateScenario(DataEngineRequest):
    uuid: str
    payload: dict

    def make_request(self):
        return {
            "method": "PUT",
            "url": urljoin("scenarios", self.uuid),
            "json": self.payload,
        }


@dataclasses.dataclass
class DeleteScenario(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin("scenarios", self.uuid),
        }


@dataclasses.dataclass
class CreateTimeline(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin("scenarios", self.uuid, "timeline"),
        }


@dataclasses.dataclass
class DeleteTimeline(DataEngineRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin("scenarios", self.uuid, "timeline"),
        }


@dataclasses.dataclass
class GetSimulation(SimulationControlRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "GET",
            "url": urljoin("simulations", self.uuid),
        }


@dataclasses.dataclass
class RunSimulation(SimulationControlRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin("simulations", self.uuid),
        }


@dataclasses.dataclass
class DeleteSimulation(SimulationControlRequest):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin("simulations", self.uuid),
        }


@dataclasses.dataclass
@unwrap_envelope("updates")
class GetUpdates(DataEngineRequest):
    scenario_uuid: str

    @simple_request
    def make_request(self):
        return urljoin("scenarios", self.scenario_uuid, "updates")


@dataclasses.dataclass
class GetSingleUpdate(DataEngineRequest):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin("updates", self.uuid)


@dataclasses.dataclass
class CreateUpdate(DataEngineRequest):
    scenario_uuid: str
    payload: dict

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin("scenarios", self.scenario_uuid, "updates"),
            "json": self.payload,
        }


@unwrap_envelope("dataset_types")
class GetDatasetTypes(DataEngineRequest):
    @simple_request
    def make_request(self):
        return urljoin("schema/dataset_types")
