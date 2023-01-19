import gimme

from movici_api_client.api.client import AsyncClient
from movici_api_client.api.requests import GetDatasets, GetProjects, GetScenarios
from movici_api_client.cli.exceptions import InvalidResource
from movici_api_client.cli.utils import validate_uuid


class ResourceQuery:
    resource_type = "resource"
    client: AsyncClient = gimme.attribute(AsyncClient)

    def request_all(self):
        raise NotImplementedError

    async def get_uuid(self, name_or_uuid):
        return (
            name_or_uuid if validate_uuid(name_or_uuid) else await self.assert_uuid(name_or_uuid)
        )

    async def get_uuids(self):
        request = self.request_all()
        all_resources = await self.client.request(request)
        return {p["name"]: p["uuid"] for p in all_resources}

    async def get_by_name_or_uuid(self, name_or_uuid):
        request = self.request_all()
        all_resources = await self.client.request(request)
        return self.get_from_list(name_or_uuid, all_resources)

    async def assert_uuid(self, name_or_uuid):
        resources = await self.get_uuids()
        try:
            return resources[name_or_uuid]
        except KeyError:
            raise InvalidResource(self.resource_type, name_or_uuid)

    def get_from_list(self, name_or_uuid, all_resources):
        match_field = "uuid" if validate_uuid(name_or_uuid) else "name"
        for res in all_resources:
            if name_or_uuid == res[match_field]:
                return res
        else:
            raise InvalidResource(self.resource_type, name_or_uuid)


class ProjectQuery(ResourceQuery):
    resource_type = "project"

    def request_all(self):
        return GetProjects()


class DatasetQuery(ResourceQuery):
    def __init__(self, project_uuid: str):
        self.project_uuid = project_uuid

    def request_all(self):
        return GetDatasets(self.project_uuid)


class ScenarioQuery(ResourceQuery):
    def __init__(self, project_uuid: str):
        self.project_uuid = project_uuid

    def request_all(self):
        return GetScenarios(self.project_uuid)
