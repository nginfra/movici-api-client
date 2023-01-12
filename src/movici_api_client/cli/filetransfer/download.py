from __future__ import annotations

import contextlib
import pathlib
import shutil
import typing as t

from tqdm.auto import tqdm

from movici_api_client.api.client import AsyncClient
from movici_api_client.api.common import Request
from movici_api_client.api.requests import (
    GetDatasetData,
    GetDatasets,
    GetScenarios,
    GetSingleScenario,
    GetSingleUpdate,
    GetUpdates,
)

from ..exceptions import InvalidFile
from ..utils import echo
from .common import ParallelTaskGroup, Task, resolve_question_flag


class DownloadResource(Task):
    EXTENSIONS = {
        "application/json": ".json",
        "application/msgpack": ".msgpack",
        "application/x-msgpack": ".msgpack",
        "application/netcdf": ".nc",
        "application/x-netcdf": ".nc",
        "text/csv": ".csv",
        "text/html": ".html",
        "text/plain": ".txt",
        "image/tiff": ".tif",
    }

    def __init__(
        self,
        client: AsyncClient,
        directory: pathlib.Path,
        name: str,
        request: Request,
        overwrite: t.Optional[bool] = None,
        progress=True,
        infer_extension=True,
        continue_after_failed_overwrite=False,
    ) -> None:
        super().__init__(client)
        self.directory = directory
        self.name = name
        self.request = request
        self.overwrite = overwrite
        self.progress = progress
        self.infer_extension = infer_extension
        self.continue_after_failed_overwrite = continue_after_failed_overwrite

    async def run(self):
        file = self.directory.joinpath(self.name)
        return await self.download_as_file(file=file)

    async def download_as_file(self, file: pathlib.Path) -> bool:
        async with self.client.stream(self.request) as response:
            file = self.file_with_suffix(file, response)
            if not self.prepare_overwrite_file(file, self.overwrite):
                return self.continue_after_failed_overwrite
            fopen = open(file, "wb")
            if self.progress:
                context = tqdm.wrapattr(
                    fopen,
                    "write",
                    miniters=1,
                    desc=file.name,
                    total=self.infer_file_size(response),
                )
            else:
                context = contextlib.nullcontext(fopen)

            with context as fout:
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    fout.write(chunk)
        return True

    @staticmethod
    def prepare_overwrite_file(file: pathlib.Path, overwrite: t.Optional[bool] = None):
        if file.exists():
            overwrite = resolve_question_flag(
                overwrite, f"File {file!s} already exists, overwrite?"
            )
            if not file.is_file():
                raise InvalidFile(msg="not a file", file=file)
            if not overwrite:
                echo(f"Cowardly refusing to overwrite existing file {file.name}")
                return False
        if not (parent := file.parent).is_dir():
            if parent.exists():
                echo(f"{parent!s} is not a valid directory")
                return False
            parent.mkdir(parents=True, exist_ok=True)
        return True

    def file_with_suffix(self, file, response):
        if self.infer_extension:
            file = file.with_suffix(
                self.EXTENSIONS.get(response.headers.get("content-type"), ".dat")
            )
        return file

    @staticmethod
    def infer_file_size(response):
        for header in ("content-length", "file-size"):
            if (result := response.headers.get(header)) is not None:
                return int(result)
        return 0


class RecursivelyDownloadResource(Task):
    def __init__(
        self,
        parent: dict,
        client: AsyncClient,
        directory: pathlib.Path,
        overwrite: t.Optional[bool] = None,
        progress=True,
        cli_params: t.Optional[dict] = None,
    ) -> None:
        super().__init__(client)
        self.parent = parent
        self.directory = directory
        self.overwrite = overwrite
        self.progress = progress
        self.cli_params = cli_params

    async def run(self):
        async with self.client:
            all_resources = await self.client.request(self.request_all())

            for task in self.create_subtasks(all_resources):
                result = await task.run()
                if result is False:
                    return

    def request_all(self):
        raise NotImplementedError

    def create_subtasks(self, resources: t.List[dict]) -> t.Iterable[Task]:
        raise NotImplementedError


class DownloadDatasets(RecursivelyDownloadResource):
    def request_all(self):
        return GetDatasets(self.parent["uuid"])

    def create_subtasks(self, resources: t.List[dict]) -> t.Iterable[t.Iterable[Task]]:
        yield from (
            DownloadResource(
                client=self.client,
                directory=self.directory,
                name=ds["name"],
                request=GetDatasetData(ds["uuid"]),
                overwrite=self.overwrite,
                progress=self.progress,
                continue_after_failed_overwrite=True,
            )
            for ds in resources
        )


class DownloadScenarios(RecursivelyDownloadResource):
    def request_all(self):
        return GetScenarios(self.parent["uuid"])

    def create_subtasks(self, resources: t.List[dict]) -> t.Iterable[t.Iterable[Task]]:
        yield ParallelTaskGroup(
            (
                DownloadSingleScenario(
                    parent=r,
                    client=self.client,
                    directory=self.directory,
                    overwrite=self.overwrite,
                    cli_params=self.cli_params,
                )
                for r in resources
            ),
            progress=self.progress,
            description="Downloading scenarios",
        )


class DownloadSingleScenario(RecursivelyDownloadResource):
    def request_all(self):
        return GetUpdates(self.parent["uuid"])

    def create_subtasks(self, resources: t.List[dict]) -> t.Iterable[t.Iterable[Task]]:
        name, uuid = self.parent["name"], self.parent["uuid"]
        yield DownloadResource(
            client=self.client,
            directory=self.directory,
            name=name,
            request=GetSingleScenario(uuid),
            overwrite=self.overwrite,
            progress=False,
        )
        if self.should_download_updates():
            directory = self.directory.joinpath(name)
            yield PrepareOverwriteDirectory(directory, overwrite=self.overwrite)
            yield ParallelTaskGroup(
                (
                    DownloadResource(
                        client=self.client,
                        directory=directory,
                        name=f"t{r['timestamp']}_{r['iteration']}_{r['name']}",
                        request=GetSingleUpdate(r["uuid"]),
                        overwrite=self.overwrite,
                        progress=False,
                    )
                    for r in resources
                ),
                progress=self.progress,
                description=name,
            )

    def should_download_updates(self):
        return self.cli_params.get("with_simulation", False)


class PrepareOverwriteDirectory(Task):
    def __init__(self, directory: pathlib.Path, overwrite: t.Optional[bool] = None):
        super().__init__(None)
        self.directory = directory
        self.overwrite = overwrite

    async def run(self):
        if not self.directory.exists():
            self.directory.mkdir(parents=True, exist_ok=True)

        try:
            is_empty_dir = not list(self.directory.iterdir())
        except NotADirectoryError:
            echo(f"{self.directory!s} is not a valid directory")
            return False

        if is_empty_dir:
            return True

        overwrite = resolve_question_flag(
            self.overwrite, f"Directory {self.directory!s} already existing, overwrite?"
        )
        if not overwrite:
            echo(f"Cowardly refusing to overwrite existing directory {self.directory!s}")
            return False
        shutil.rmtree(self.directory)
        self.directory.mkdir()
        return True
