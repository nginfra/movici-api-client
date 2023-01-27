import gimme

from movici_api_client.api.client import AsyncClient
from movici_api_client.api.requests import (
    GetDatasets,
    GetProjects,
    GetScenarios,
    GetScopes,
    GetViews,
)
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
        all_resources = await self.get_all()
        return {p["name"]: p["uuid"] for p in all_resources}

    async def by_name_or_uuid(self, name_or_uuid):
        all_resources = await self.get_all()
        return self.get_from_list(name_or_uuid, all_resources)

    async def get_all(self):
        request = self.request_all()
        return await self.client.request(request)

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


class ScopeQuery(ResourceQuery):
    resource_type = "scope"

    def request_all(self):
        return GetScopes()


class DatasetQuery(ResourceQuery):
    resource_type = "dataset"

    def __init__(self, project_uuid: str):
        self.project_uuid = project_uuid

    def request_all(self):
        return GetDatasets(self.project_uuid)


class ScenarioQuery(ResourceQuery):
    resource_type = "scenario"

    def __init__(self, project_uuid: str):
        self.project_uuid = project_uuid

    def request_all(self):
        return GetScenarios(self.project_uuid)


class ViewQuery(ResourceQuery):
    resource_type = "view"

    def __init__(self, project_uuid: str, scenario_name_or_uuid: str):
        self.project_uuid = project_uuid
        self.scenario_name_or_uuid = scenario_name_or_uuid

    async def get_all(self):
        scenario_uuid = await ScenarioQuery(self.project_uuid).get_uuid(self.scenario_name_or_uuid)
        return await self.client.request(GetViews(scenario_uuid))
