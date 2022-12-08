from movici_api_client.api.client import Client
from movici_api_client.api.requests import GetProjects, GetSingleProject
from movici_api_client.cli.dependencies import get
from movici_api_client.cli.ui import format_dict, format_object
from movici_api_client.cli.utils import authenticated, echo
from ..common import  Controller
from ..utils import argument, assert_project_uuid, command, tabulate_results, validate_uuid


class ProjectController(Controller):
    name = "project"

    @command(name="projects")
    @authenticated
    @tabulate_results(keys=("uuid", "name", "display_name", "created_on"))
    def list(self):
        client = get(Client)
        return client.request(GetProjects())['projects']

    @command
    @argument("name_or_uuid")
    @authenticated
    def get(self, name_or_uuid):
        client = get(Client)

        if validate_uuid(name_or_uuid):
            uuid = name_or_uuid
        else:
            uuid = assert_project_uuid(name_or_uuid)
            
        result= client.request(GetSingleProject(uuid))
        echo(format_object(result, fields=("uuid", "name", "display_name", "created_on")))
            
        

    @command
    @authenticated
    def create(self):
        ...

    @command
    @authenticated
    def update(self):
        ...

    @command
    @authenticated
    def delete(self):
        ...
