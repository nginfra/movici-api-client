from __future__ import annotations

import contextlib
import logging
import typing as t
from asyncio import Semaphore

import httpx
from httpx import HTTPError, HTTPStatusError, Response, Timeout  # noqa

from .common import Auth, BaseClient, BaseRequest, ErrorCallback, IAsyncClient, ISyncClient, Service

T = t.TypeVar("T")

DEFAULT_TIMEOUT_CONFIG = Timeout(timeout=5.0)


class Client(BaseClient, ISyncClient):
    def __init__(
        self,
        base_url: str,
        auth: Auth | None | False = None,
        client: httpx.Client | None = None,
        logger: logging.Logger | None = None,
        on_error: ErrorCallback | None = None,
        service_urls: dict[Service, str] | None = None,
    ):
        super().__init__(base_url, auth, logger, on_error, service_urls)
        self.client = client or httpx.Client(timeout=DEFAULT_TIMEOUT_CONFIG)
        self.timeout = self.client.timeout

    def request(
        self,
        req: BaseRequest[T],
        on_error: ErrorCallback | None = None,
    ) -> T | None:
        self._assert_auth(req)
        conf = self._prepare_request_config(req)
        resp = self.client.request(**conf)
        self._handle_failure(resp, on_error)
        return req.make_response(resp)

    @contextlib.contextmanager
    def stream(self, req: BaseRequest[T], on_error: ErrorCallback | None = None):
        conf = self._prepare_request_config(req)
        with self.client.stream(**conf) as resp:
            self._handle_failure(resp, on_error)
            yield resp


class AsyncClient(BaseClient, IAsyncClient):
    client: httpx.AsyncClient | None

    def __init__(
        self,
        base_url: str,
        auth: Auth | None | False = None,
        client_factory: type[httpx.AsyncClient] = httpx.AsyncClient,
        logger: logging.Logger | None = None,
        on_error: ErrorCallback | None = None,
        service_urls: dict[Service, str] | None = None,
        max_concurrent=10,
        timeout=DEFAULT_TIMEOUT_CONFIG,
    ):
        super().__init__(base_url, auth, logger, on_error, service_urls)
        self.client_factory = client_factory
        self.client = None
        self.concurrent_requests = Semaphore(max_concurrent)
        self.enter_count = 0
        self.timeout = timeout

    async def request(self, req: BaseRequest[T], on_error: ErrorCallback | None = None):
        async with self.concurrent_requests:
            self._ensure_client()
            self._assert_auth(req)
            conf = self._prepare_request_config(req)
            resp = await self.client.request(**conf)

            self._handle_failure(resp, on_error)
            return req.make_response(resp)

    @contextlib.asynccontextmanager
    async def stream(self, req: BaseRequest[T], on_error: ErrorCallback | None = None):
        async with self.concurrent_requests:
            self._ensure_client()
            conf = self._prepare_request_config(req)
            async with self.client.stream(**conf) as resp:
                self._handle_failure(resp, on_error)
                yield resp

    def _ensure_client(self):
        if self.client is None:
            self.client = self.client_factory(timeout=self.timeout)
        return self.client

    async def close(self):
        if self.client is None:
            return

        await self.client.aclose()
        self.client = None

    async def __aenter__(self):
        if self.client is None:
            self.client = self.client_factory()
        self.enter_count += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.enter_count -= 1
        if self.enter_count <= 0:
            await self.close()

    @classmethod
    def from_sync_client(cls, client: Client):
        return AsyncClient(
            base_url=client.base_url,
            auth=client.auth,
            logger=client.logger,
            on_error=client.on_error,
            service_urls=client.service_urls,
            timeout=client.timeout,
        )
