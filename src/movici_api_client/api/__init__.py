import logging
from .common import BaseApi, Auth, BaseRequest
import typing as t
import httpx

T = t.TypeVar("T")


class Api(BaseApi):
    def __init__(
        self,
        base_url: str,
        auth: t.Optional[Auth] = None,
        client: t.Optional[httpx.Client] = None,
        logger: t.Optional[logging.Logger] = None,
    ):
        self.base_url = base_url
        self.auth = auth
        self.client = client or httpx.Client()
        self.logger = logger

    def request(
        self, req: BaseRequest[T], on_error: t.Callable[[httpx.Response], t.Any] = None
    ) -> t.Optional[T]:
        if req.auth:
            if self.auth is None:
                raise ValueError(
                    "request is authenticated, but no authenticationprovider configured"
                )
        conf = req.generate_config(self)
        conf = self.auth(conf)
        if self.logger:
            self.logger.debug(str(conf))
        resp = self.client.request(**conf)
        if resp.status_code >= 400:
            if on_error:
                on_error(resp)
            else:
                resp.raise_for_status()
        return req.make_response(resp)
