from __future__ import annotations
import contextlib
import logging
from .common import BaseApi, Auth, BaseRequest
import typing as t
import httpx
from httpx import Response  # do not remove, this is exported to consumers

T = t.TypeVar("T")

ErrorCallback = t.Callable[[httpx.Response], bool]


class Client(BaseApi):
    def __init__(
        self,
        base_url: str,
        auth: t.Optional[Auth] = None,
        client: t.Optional[httpx.Client] = None,
        logger: t.Optional[logging.Logger] = None,
        on_error: t.Optional[ErrorCallback] = None,
    ):
        self.base_url = base_url
        self.auth = auth
        self.client = client or httpx.Client()
        self.logger = logger
        self.on_error = on_error

    def request(
        self, req: BaseRequest[T], on_error: t.Optional[ErrorCallback] = None
    ) -> t.Optional[T]:
        if req.auth:
            if self.auth is None:
                raise ValueError(
                    "request is authenticated, but no authenticationprovider configured"
                )
        conf = self._prepare_request_config(req)
        resp = self.client.request(**conf)
        self._handle_failure(resp, on_error)
        return req.make_response(resp)

    @contextlib.contextmanager
    def stream(self, req: BaseRequest[T], on_error: t.Optional[ErrorCallback] = None):
        conf = self._prepare_request_config(req)
        with self.client.stream(**conf) as resp:
            self._handle_failure(resp)
            yield resp

    def _prepare_request_config(self, req: BaseRequest[T]):
        conf = req.generate_config(self)
        conf = self.auth(conf)
        if self.logger:
            self.logger.debug(str(conf))
        return conf

    def _handle_failure(self, resp: Response, on_error: t.Optional[ErrorCallback] = None):
        if resp.status_code >= 400:
            run_global_error_callback = True
            if on_error:
                # a "local" error callback can return False to indicate that all error handling
                # has been completed and the global error handling should not take place. We need
                # to specifcally look for False and not just Falsy (ie: None)
                run_global_error_callback = on_error(resp) is not False
            if not run_global_error_callback:
                return
            if self.on_error:
                self.on_error(resp)
            else:
                resp.raise_for_status()


class AsyncClient(BaseApi):
    ...
