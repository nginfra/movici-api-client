from __future__ import annotations

import enum
import functools
import typing as t
from functools import reduce
from urllib.parse import urljoin as urljoin_

from httpx import Response


class MoviciServiceUnavailable(Exception):
    pass


class Service(enum.Enum):
    AUTH = ("auth", "/auth/v1/")
    DATA_ENGINE = ("data_engine", "/data-engine/v4/")
    MODEL_ENGINE = ("model_engine", "/model-engine/v1/")

    @classmethod
    def by_name(cls):
        return {s.value[0]: s for s in cls}

    @classmethod
    def urls(cls):
        return {s: s.value[1] for s in cls}


class Auth:
    def login(self, api: BaseApi):
        raise NotImplementedError

    def __call__(self, config: dict) -> dict:
        raise NotImplementedError


class BaseApi:
    def resolve_service_url(self, service: t.Optional[Service]) -> str:
        raise NotImplementedError


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
    service: t.Optional[Service] = None

    def generate_config(self, api: BaseApi):
        request = self.make_request()
        base_url = api.resolve_service_url(self.service)
        return {
            **request,
            "url": urljoin(base_url, request["url"]),
        }


def simple_request(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return {"method": "GET", "url": func(*args, **kwargs)}

    return wrapped


def unwrap_envelope(envelope):
    def decorator(cls: Request):
        def make_response(self, resp: Response):
            return resp.json()[envelope]

        cls.make_response = make_response
        return cls

    return decorator


def urljoin(*parts):
    return reduce(urljoin_, (str(part) + "/" for part in parts))


def pick(obj, attrs: t.List[str], default=None):
    return {key: getattr(obj, key, default) for key in attrs}
