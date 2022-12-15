import dataclasses
import io
import pathlib
from .common import Request, simple_request, unwrap_envelope, urljoin, pick, Response
import typing as t


class APIBase:
    AUTH = "/auth/v1/"
    DATA_ENGINE = "/data-engine/v4/"
    MODEL_ENGINE = "/model-engine/v1/"


@dataclasses.dataclass
class Login(Request):
    auth = False
    username: str
    password: str

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin(APIBase.AUTH, "user", "login"),
            "json": pick(self, ["username", "password"]),
        }


@dataclasses.dataclass
class CheckAuthToken(Request):
    auth_token: t.Optional[str] = None

    def make_request(self):
        req = {
            "method": "GET",
            "url": urljoin(APIBase.AUTH, "auth"),
        }
        if self.auth_token is not None:
            req["headers"] = {"Authorization": self.auth_token}
        return req


class GetProjects(Request):
    @simple_request
    def make_request(self):
        return urljoin(APIBase.DATA_ENGINE, "projects")

    def make_response(self, resp: Response):
        return resp.json()["projects"]


@dataclasses.dataclass
class GetSingleProject(Request):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin(APIBase.DATA_ENGINE, "projects", self.uuid)


@dataclasses.dataclass
class CreateProject(Request):
    name: str
    display_name: str

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin(APIBase.DATA_ENGINE, "projects"),
            "json": pick(self, ["name", "display_name"]),
        }


@dataclasses.dataclass
class UpdateProject(Request):
    uuid: str
    display_name: str

    def make_request(self):
        return {
            "method": "PUT",
            "url": urljoin(APIBase.DATA_ENGINE, "projects", self.uuid),
            "json": pick(self, ["display_name"]),
        }


@dataclasses.dataclass
class DeleteProject(Request):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin(APIBase.DATA_ENGINE, "projects", self.uuid),
        }


@dataclasses.dataclass
class GetDatasets(Request):
    project_uuid: str

    @simple_request
    def make_request(self):
        return urljoin(APIBase.DATA_ENGINE, "projects", self.project_uuid, "datasets")

    def make_response(self, resp: Response):
        return resp.json()["datasets"]


@dataclasses.dataclass
class GetSingleDataset(Request):
    uuid: str

    @simple_request
    def make_request(self):
        return urljoin(APIBase.DATA_ENGINE, "datasets", self.uuid)


@dataclasses.dataclass
class CreateDataset(Request):
    project_uuid: str
    name: str
    type: str
    display_name: t.Optional[str] = None

    def make_request(self):
        return {
            "method": "POST",
            "url": urljoin(APIBase.DATA_ENGINE, "projects", self.project_uuid, "datasets"),
            "json": pick(self, ("name", "type", "display_name")),
        }


@dataclasses.dataclass
class DeleteDataset(Request):
    uuid: str

    def make_request(self):
        return {"method": "DELETE", "url": urljoin(APIBase.DATA_ENGINE, "datasets", self.uuid)}


@dataclasses.dataclass
class AddDatasetData(Request):
    uuid: str
    file: t.Union[str, pathlib.Path, io.BufferedIOBase]

    def make_request(self):
        file = self.file
        if isinstance(file, (str, pathlib.Path)):
            file = open(file, "rb")

        return {
            "method": "POST",
            "url": urljoin(APIBase.DATA_ENGINE, "datasets", self.uuid, "data"),
            "files": {
                "data": file,
            },
        }


class ModifiyDatasetData(AddDatasetData):
    def make_request(self):
        return {**super().make_request(), "method": "PUT"}


@dataclasses.dataclass
class DeleteDatasetData(Request):
    uuid: str

    def make_request(self):
        return {
            "method": "DELETE",
            "url": urljoin(APIBase.DATA_ENGINE, "datasets", self.uuid, "data"),
        }


@unwrap_envelope("dataset_types")
class GetDatasetTypes(Request):
    @simple_request
    def make_request(self):
        return urljoin(APIBase.DATA_ENGINE, "schema/dataset_types")
