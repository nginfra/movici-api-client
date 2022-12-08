from movici_api_client.api.client import Client
from movici_api_client.api.requests import GetProjects
from movici_api_client.cli.dependencies import get
from movici_api_client.cli.utils import authenticated, echo
from ..common import  Controller
from ..utils import command


class ProjectController(Controller):
    name = "project"

    @command(name="projects")
    @authenticated
    def list(self):
        client = get(Client)
        echo(client.request(GetProjects()))

    @command
    @authenticated
    def get(self):
        ...

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
