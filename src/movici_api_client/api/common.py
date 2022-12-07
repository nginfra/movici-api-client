from __future__ import annotations
import functools
from urllib.parse import urljoin as urljoin_
from functools import reduce
import typing as t
from httpx import Response


class Auth:
    def login(self, api: BaseApi):
        raise NotImplementedError

    def __call__(self, config: dict) -> dict:
        raise NotImplementedError


class BaseApi:
    base_url: str


T = t.TypeVar("T")


class BaseRequest(t.Generic[T]):
    auth = False

    def generate_config(self, api: BaseApi):
        return self.make_request()

    def make_request(self):
        raise NotImplementedError

    def make_response(self, resp: Response) -> T:
        return resp.json()


class Request(BaseRequest):
    auth = True

    def generate_config(self, api: BaseApi):
        request = self.make_request()
        return {
            **request,
            "url": urljoin(api.base_url, request["url"]),
        }


def simple_request(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return {"method": "GET", "url": func(*args, **kwargs)}

    return wrapped


def urljoin(*parts):
    return reduce(urljoin_, (str(part) + "/" for part in parts))


def pick(obj, attrs: t.List[str]):
    return {key: getattr(obj, key) for key in attrs}
