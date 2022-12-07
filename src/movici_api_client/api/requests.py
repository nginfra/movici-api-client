import dataclasses
from .common import Request, simple_request, urljoin, pick
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
