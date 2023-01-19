import typing as t

from movici_api_client.api import requests as req
from movici_api_client.api.client import AsyncClient
from movici_api_client.cli.filetransfer.download import DownloadDatasets, DownloadResource
from movici_api_client.cli.filetransfer.upload import (
    DatasetUploadStrategy,
    UploadMultipleResources,
    UploadResource,
)
from movici_api_client.cli.helpers import edit_resource

from ..cqrs import Event, EventHandler, Mediator
from ..events.dataset import (
    ClearDataset,
    CreateDataset,
    DeleteDataset,
    DownloadDataset,
    DownloadMultipleDatasets,
    EditDataset,
    GetAllDatasets,
    GetDatasetTypes,
    GetSingleDataset,
    UpdateDataset,
    UploadDataset,
    UploadMultipleDatasets,
)
from ..events.project import GetAllProjects
from ..exceptions import InvalidActiveProject, NoActiveProject, NoChangeDetected
from ..handlers.query import DatasetQuery
from ..utils import assert_current_context, confirm, prompt_choices_async


def requires_valid_project_uuid(cls: t.Type[EventHandler]):
    original = cls.handle

    async def handle(self, event: Event, mediator: Mediator):
        context = assert_current_context()
        proj_name = context.get("project")
        if not proj_name:
            raise NoActiveProject()
        projects = await mediator.send(GetAllProjects())
        projects_dict = {p["name"]: p["uuid"] for p in projects}
        try:
            self.project_uuid = projects_dict[proj_name]
        except KeyError:
            raise InvalidActiveProject(proj_name)

        return await original(self, event, mediator)

    cls.handle = handle
    return cls


class RemoteEventHandler(EventHandler):
    def __init__(self, client: AsyncClient) -> None:
        self.client = client


class RemoteRequestHandler(RemoteEventHandler):
    async def handle(self, event: Event, mediator: Mediator):
        request = await self.make_request(event)
        return await self.client.request(request)

    async def make_request(self, event):
        raise NotImplementedError


class RemoteGetAllProjectsHandler(RemoteRequestHandler):
    __event__ = GetAllProjects

    async def make_request(self, event):
        return req.GetProjects()


class RemoteGetDatasetTypesHandler(RemoteRequestHandler):
    __event__ = GetDatasetTypes

    async def make_request(self, event):
        return req.GetDatasetTypes()


@requires_valid_project_uuid
class RemoteGetAllDatasetsHandler(RemoteRequestHandler):
    __event__ = GetAllDatasets
    project_uuid: str

    async def make_request(self, event):
        return req.GetDatasets(project_uuid=self.project_uuid)


@requires_valid_project_uuid
class RemoteGetSingleDatasetHandler(RemoteRequestHandler):
    __event__ = GetSingleDataset
    project_uuid: str

    async def make_request(self, event: GetSingleDataset):
        uuid = await DatasetQuery(self.project_uuid).get_uuid(event.name_or_uuid)
        return req.GetSingleDataset(uuid)


@requires_valid_project_uuid
class RemoteCreateDatasetHandler(RemoteEventHandler):
    __event__ = CreateDataset
    project_uuid: str

    async def handle(self, event: CreateDataset, mediator: Mediator):
        async with self.client:
            if event.type is None:
                all_types = await mediator.send(GetDatasetTypes())
                event.type = await prompt_choices_async(
                    "Type", sorted([tp["name"] for tp in all_types])
                )
            return await self.client.request(
                req.CreateDataset(
                    project_uuid=self.project_uuid,
                    name=event.name,
                    type=event.type,
                    display_name=event.display_name,
                )
            )


@requires_valid_project_uuid
class RemoteUpdateDatasetHandler(RemoteEventHandler):
    __event__ = UpdateDataset
    project_uuid: str

    async def handle(self, event: UpdateDataset, mediator: Mediator):
        async with self.client:
            uuid = await DatasetQuery(self.project_uuid).get_uuid(event.name_or_uuid)
            if not any((event.name, event.type, event.display_name)):
                raise NoChangeDetected()
            return await self.client.request(
                req.UpdateDataset(
                    uuid, name=event.name, type=event.type, display_name=event.display_name
                )
            )


@requires_valid_project_uuid
class RemoteDeleteDatasetHandler(RemoteEventHandler):
    __event__ = DeleteDataset
    project_uuid: str

    async def handle(self, event: DeleteDataset, mediator: Mediator):
        async with self.client:
            uuid = await DatasetQuery(self.project_uuid).get_uuid(event.name_or_uuid)

            confirm(
                f"Are you sure you wish to delete dataset '{event.name_or_uuid}' and all its data?"
            )
            return await self.client.request(req.DeleteDataset(uuid))


@requires_valid_project_uuid
class RemoteClearDatasetHandler(RemoteEventHandler):
    __event__ = ClearDataset
    project_uuid: str

    async def handle(self, event: ClearDataset, mediator: Mediator):
        async with self.client:
            uuid = await DatasetQuery(self.project_uuid).get_uuid(event.name_or_uuid)

            confirm(
                f"Are you sure you wish to clear dataset '{event.name_or_uuid}' of all its data?"
            )
            return await self.client.request(req.DeleteDatasetData(uuid))


@requires_valid_project_uuid
class RemoteUploadDatasetHandler(RemoteEventHandler):
    __event__ = UploadDataset
    project_uuid: str

    async def handle(self, event: UploadDataset, mediator: Mediator):
        return await UploadResource(
            file=event.file,
            parent_uuid=self.project_uuid,
            strategy=DatasetUploadStrategy(self.client),
            name_or_uuid=event.name_or_uuid,
        )


@requires_valid_project_uuid
class RemoteUploadMultipleDatasetsHandler(RemoteEventHandler):
    __event__ = UploadMultipleDatasets
    project_uuid: str

    async def handle(self, event: UploadMultipleDatasets, mediator: Mediator):
        return await UploadMultipleResources(
            event.directory, self.project_uuid, strategy=DatasetUploadStrategy(client=self.client)
        )


@requires_valid_project_uuid
class RemoteDownloadDatasetHandler(RemoteEventHandler):
    __event__ = DownloadDataset
    project_uuid: str

    async def handle(self, event: DownloadDataset, mediator: Mediator):
        dataset = await DatasetQuery(self.project_uuid).by_name_or_uuid(event.name_or_uuid)
        file = event.directory.datasets.joinpath(dataset["name"])
        return await DownloadResource(
            file=file,
            request=req.GetDatasetData(dataset["uuid"]),
        )


@requires_valid_project_uuid
class RemoteDownloadMultipleDatasets(RemoteEventHandler):
    __event__ = DownloadMultipleDatasets
    project_uuid: str

    async def handle(self, event: DownloadMultipleDatasets, mediator: Mediator):
        await DownloadDatasets(
            {"uuid": self.project_uuid},
            directory=event.directory,
        )


@requires_valid_project_uuid
class RemoteEditDataset(RemoteEventHandler):
    __event__ = EditDataset
    project_uuid: str

    async def handle(self, event: EditDataset, mediator: Mediator):
        current = await mediator.send(GetSingleDataset(event.name_or_uuid))
        uuid = current["uuid"]
        result = edit_resource(current)
        await self.client.request(req.UpdateDataset(uuid, payload=result))


ALL_HANDLERS = (
    RemoteRequestHandler,
    RemoteGetAllProjectsHandler,
    RemoteGetDatasetTypesHandler,
    RemoteGetAllDatasetsHandler,
    RemoteGetSingleDatasetHandler,
    RemoteCreateDatasetHandler,
    RemoteUpdateDatasetHandler,
    RemoteDeleteDatasetHandler,
    RemoteClearDatasetHandler,
    RemoteUploadDatasetHandler,
    RemoteUploadMultipleDatasetsHandler,
    RemoteDownloadDatasetHandler,
    RemoteDownloadMultipleDatasets,
    RemoteEditDataset,
)
