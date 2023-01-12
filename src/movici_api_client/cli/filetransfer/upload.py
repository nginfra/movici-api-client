from __future__ import annotations

import contextlib
import pathlib
import re
import typing as t

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from movici_api_client.api import IAsyncClient
from movici_api_client.api.requests import (
    AddDatasetData,
    CreateDataset,
    CreateScenario,
    CreateTimeline,
    CreateUpdate,
    DeleteTimeline,
    GetDatasets,
    GetDatasetTypes,
    GetScenarios,
    GetSingleScenario,
    ModifiyDatasetData,
    UpdateScenario,
)

from ..exceptions import InvalidFile, InvalidResource
from ..helpers import read_json_file
from ..utils import echo, prompt_choices_async, validate_uuid
from .common import ParallelTaskGroup, Task, resolve_question_flag


class UploadResource(Task):
    def __init__(
        self,
        client: IAsyncClient,
        file: pathlib.Path,
        parent_uuid: str,
        name_or_uuid: t.Optional[str] = None,
        overwrite=None,
        create_new=None,
        inspect_file=None,
        all_resources: t.Optional[t.Sequence[dict]] = None,
        strategy: UploadStrategy = None,
        **kwargs,
    ) -> None:

        super().__init__(client)
        self.file = file
        self.parent_uuid = parent_uuid
        self.name_or_uuid = name_or_uuid
        self.overwrite = overwrite
        self.create_new = create_new
        self.inspect_file = inspect_file
        self.all_resources = all_resources
        self.strategy = strategy
        self.kwargs = kwargs

    async def run(self) -> t.Optional[bool]:
        self.ensure_strategy()
        strategy = self.strategy
        async with self.client:
            name, existing = await self.get_existing()

            if not existing:
                if self.determine_create_new(self.create_new, name):
                    return await strategy.create_new(
                        self.parent_uuid, file=self.file, name=name, inspect_file=self.inspect_file
                    )

            if strategy.require_overwrite_question(existing) and not self.determine_overwrite(
                self.overwrite, name
            ):
                return

            return await strategy.update_existing(existing, self.file, self.inspect_file)

    async def ensure_all_resources(self):
        if self.all_resources is None:
            self.all_resources = await self.strategy.get_all(self.parent_uuid)

    async def get_existing(self) -> t.Optional[dict]:
        await self.ensure_all_resources()
        name_or_uuid, all_resources = self.name_or_uuid, self.all_resources
        if not name_or_uuid:
            name_or_uuid = self.file.stem
        match_field = "uuid" if validate_uuid(name_or_uuid) else "name"
        for res in all_resources:
            if name_or_uuid == res[match_field]:
                return res["name"], res
        if match_field == "uuid":
            InvalidResource("scenario", name_or_uuid)
        return name_or_uuid, None

    def determine_create_new(self, create_new, name):
        resource_type = self.strategy.resource_type
        do_create = resolve_question_flag(
            create_new,
            (
                f"{resource_type.capitalize()} {name} does not exist, "
                f"do you wish to create this {resource_type}?"
            ),
        )
        if not do_create:
            echo(f"Cowardly refusing to create new {resource_type} {name}")
        return do_create

    def determine_overwrite(self, overwrite, name):
        resource_type = self.strategy.resource_type

        do_overwrite = resolve_question_flag(
            overwrite,
            (
                f"{resource_type.capitalize()} {name} already has data, "
                "do you wish to overwrite?"
            ),
        )
        if not do_overwrite:
            echo(f"Cowardly refusing to overwrite data for {resource_type} '{name}'")
        return do_overwrite

    def ensure_strategy(self):
        if self.strategy is None:
            self.strategy = self.make_strategy()

    def make_strategy(self) -> UploadStrategy:
        raise NotImplementedError


class UploadMultipleResources(Task):
    def __init__(
        self,
        client: IAsyncClient,
        directory: pathlib.Path,
        parent_uuid: str,
        strategy: UploadStrategy = None,
        upload_task=UploadResource,
        **kwargs,
    ):
        super().__init__(client)
        self.directory = directory
        self.parent_uuid = parent_uuid
        self.strategy = strategy
        self.upload_task = upload_task
        self.kwargs = kwargs

    async def run(self) -> t.Optional[bool]:
        all_resources = await self.strategy.get_all(self.parent_uuid)
        async with self.client:
            for file in tqdm(
                list(self.strategy.iter_files(self.directory)),
                desc=f"Processing {self.strategy.resource_type} files",
            ):
                task = self.upload_task(
                    client=self.client,
                    file=file,
                    parent_uuid=self.parent_uuid,
                    all_resources=all_resources,
                    strategy=self.strategy,
                    description=file.stem,
                    **self.kwargs,
                )
                await task.run()


class UploadDataset(UploadResource):
    def make_strategy(self):
        return DatasetUploadStrategy(self.client)


class UploadScenario(Task):
    def __init__(
        self,
        client: IAsyncClient,
        file: pathlib.Path,
        parent_uuid: str,
        name_or_uuid=None,
        overwrite=None,
        create_new=None,
        inspect_file=None,
        all_resources=None,
        strategy: UploadStrategy = None,
        **kwargs,
    ):
        self.client = client
        self.file = file
        self.parent_uuid = parent_uuid
        self.overwrite = overwrite
        self.create_new = create_new
        self.inspect_file = inspect_file
        self.name_or_uuid = name_or_uuid
        self.all_resources = all_resources
        self.strategy = strategy
        self.kwargs = kwargs

    async def run(self) -> t.Optional[bool]:
        strategy = self.strategy or ScenarioUploadStrategy(self.client)
        uuid = await UploadResource(
            client=self.client,
            file=self.file,
            parent_uuid=self.parent_uuid,
            overwrite=self.overwrite,
            create_new=self.create_new,
            inspect_file=self.inspect_file,
            name_or_uuid=self.name_or_uuid,
            all_resources=self.all_resources,
            strategy=strategy,
        ).run()
        if uuid is None:
            return
        if not self.kwargs.get("with_simulation"):
            return
        simulation_dir = self.file.with_suffix("")
        if not simulation_dir.is_dir():
            echo(f"Scenario {self.file.name} does not have a valid simulation")
            return
        scenario = None
        if self.all_resources is not None:
            scenarios_by_uuid = {r["uuid"]: r for r in self.all_resources}
            scenario = scenarios_by_uuid.get(uuid)
        await UploadTimeline(
            self.client,
            simulation_dir,
            parent_uuid=uuid,
            overwrite=self.overwrite,
            scenario=scenario,
            description=self.kwargs.get("description"),
        )


class UploadTimeline(Task):
    extensions = {".json"}

    def __init__(
        self,
        client: IAsyncClient,
        directory: pathlib.Path,
        parent_uuid: str,
        overwrite,
        scenario: dict = None,
        description: str = None,
    ):
        self.client = client
        self.directory = directory
        self.parent_uuid = parent_uuid
        self.overwrite = overwrite
        self.scenario = scenario
        self.description = description

    async def run(self) -> t.Optional[bool]:
        async with self.client:
            await self.recreate_timeline()
            await ParallelTaskGroup(
                (
                    UploadUpdate(self.client, self.parent_uuid, file)
                    for file in self.iter_updates()
                ),
                progress=True,
                description=self.description or "Uploading updates",
            ).run()

    async def recreate_timeline(self):
        scenario = self.scenario or await self.client.request(GetSingleScenario(self.parent_uuid))
        if scenario.get("has_timeline"):
            await self.client.request(DeleteTimeline(self.parent_uuid))
        await self.client.request(CreateTimeline(self.parent_uuid))

    def iter_updates(self):
        for file in self.directory.glob("*"):
            if not file.is_file():
                continue
            if self.extensions is not None and file.suffix not in self.extensions:
                continue
            yield file


class UploadUpdate(Task):
    def __init__(self, client: IAsyncClient, parent_uuid: str, file: pathlib.Path) -> None:
        super().__init__(client)
        self.parent_uuid = parent_uuid
        self.file = file

    async def run(self) -> t.Optional[bool]:
        try:
            payload = self.prepare_payload()
        except ValueError as e:
            echo(f"Error reading {self.file}: {e!s}", err=True)
            return
        await self.client.request(CreateUpdate(self.parent_uuid, payload))

    def prepare_payload(self) -> t.Optional[dict]:
        try:
            contents = read_json_file(self.file)
        except InvalidFile:
            raise ValueError("Not a valid update file")

        if {"name", "timestamp", "iteration"} - contents.keys():
            match = re.match(
                r"t(?P<timestamp>\d+)_(?P<iteration>\d+)_(?P<dataset>\w+)\..*", self.file.name
            )
            if not match:
                raise ValueError("Could not determine required update info")

            filename_meta = match.groupdict()
            contents.update(filename_meta)
        if "data" not in contents:
            if contents["name"] not in contents:
                raise ValueError("Could not determine update data")
            contents["data"] = contents["name"]
        return contents


class UploadStrategy:
    extensions: t.Optional[t.Collection]
    messages: dict
    resource_type: str = "resource"

    def __init__(self, client: IAsyncClient):
        self.client = client

    def iter_files(self, directory: pathlib.Path):
        for file in directory.glob("*"):
            if not file.is_file():
                continue
            if self.extensions is None or file.suffix in self.extensions:
                yield file

    def require_overwrite_question(self, existing: dict):
        return True

    async def get_all(self):
        raise NotImplementedError

    async def create_new(
        self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False
    ):
        raise NotImplementedError

    async def update_existing(
        self, existing: dict, file: pathlib.Path, name=None, inspect_file=False
    ):
        raise NotImplementedError


class DatasetUploadStrategy(UploadStrategy):
    extensions = {".json", ".msgpack", ".csv", ".nc", ".tiff", ".tif", ".geotif", ".geotif"}
    resource_type = "dataset"

    def __init__(self, client: IAsyncClient, all_dataset_types=None):
        super().__init__(client)
        self.all_dataset_types = all_dataset_types

    async def get_all(self, parent_uuid: str):
        return await self.client.request(GetDatasets(project_uuid=parent_uuid))

    def require_overwrite_question(self, existing: t.Optional[dict]):
        if existing is None:
            return False
        return existing["has_data"]

    async def create_new(
        self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False
    ):
        name = name or file.stem
        await self.ensure_all_dataset_types()
        dataset_type = await self.infer_dataset_type(file, inspect_file=inspect_file)
        uuid = (
            await self.client.request(
                CreateDataset(parent_uuid, name, type=dataset_type, display_name=name)
            )
        )["dataset_uuid"]
        await self.upload_new_data(uuid, file)
        return uuid

    async def update_existing(self, existing: dict, file: pathlib.Path, inspect_file=False):
        uuid, has_data = existing["uuid"], existing["has_data"]

        if has_data:
            await self.upload_existing_data(uuid, file)
        else:
            await self.upload_new_data(uuid, file)
        return uuid

    async def ensure_all_dataset_types(self):
        if self.all_dataset_types is None:
            self.all_dataset_types = await self.client.request(GetDatasetTypes())

    async def infer_dataset_type(self, file: pathlib.Path, inspect_file):
        if file.suffix in (".csv"):
            return "parameters"
        if file.suffix in (".nc"):
            return "flooding_tape"
        if file.suffix in (".tiff", ".tif", ".geotif", ".geotif"):
            return "height_map"
        if inspect_file:
            if file.suffix == ".json":
                try:
                    return read_json_file(file)["type"]
                except (InvalidFile, KeyError):
                    pass
        if not self.all_dataset_types:
            echo(f"Could not determine dataset type for '{file.name}'")
            return
        return await prompt_choices_async(
            f"Please specify the type for dataset '{file.name}'", self.all_dataset_types
        )

    async def upload_new_data(self, uuid, file):
        with read_file_progress_bar(file) as fobj:
            return await self.client.request(AddDatasetData(uuid, fobj))

    async def upload_existing_data(self, uuid, file):
        with read_file_progress_bar(file) as fobj:
            return await self.client.request(ModifiyDatasetData(uuid, fobj))


class ScenarioUploadStrategy(UploadStrategy):
    resource_type = "scenario"
    extensions = {".json"}

    async def get_all(self, parent_uuid: str):
        return await self.client.request(GetScenarios(project_uuid=parent_uuid))

    async def create_new(
        self, parent_uuid: str, file: pathlib.Path, name=None, inspect_file=False
    ):
        payload = self._prepare_payload(name or file.stem, file, inspect_file)
        return (await self.client.request(CreateScenario(parent_uuid, payload)))["scenario_uuid"]

    async def update_existing(
        self, existing: dict, file: pathlib.Path, name=None, inspect_file=False
    ):
        payload = self._prepare_payload(name or file.stem, file, inspect_file)
        await self.client.request(UpdateScenario(existing["uuid"], payload))
        return existing["uuid"]

    def _prepare_payload(self, name, file, inspect_file):
        payload = read_json_file(file)
        if inspect_file:
            payload["name"] = name
        return payload


@contextlib.contextmanager
def read_file_progress_bar(file: pathlib.Path):
    with open(file, "rb") as fobj, tqdm(
        total=file.stat().st_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file.name
    ) as t:
        yield CallbackIOWrapper(t.update, fobj, "read")
        t.reset()
