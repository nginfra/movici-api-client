import typing as t

from .common import Auth, BaseApi
from .requests import Login


class MoviciTokenAuth(Auth):
    auth_token: t.Optional[str]

    def __init__(self, auth_token):
        self.auth_token = auth_token

    def __call__(self, config: dict) -> dict:
        if self.auth_token is None:
            return config
        if "headers" not in config:
            config["headers"] = {}
        config["headers"]["Authorization"] = self.auth_token
        return config


class MoviciLoginAuth(MoviciTokenAuth):
    def __init__(self, username: str, password: str) -> None:
        super().__init__(None)
        self.username = username
        self.password = password

    def login(self, api: BaseApi):
        resp = api.request(Login(self.username, self.password))
        self.auth_token = resp["session"]
