from movici_api_client.api.client import Client
from movici_api_client.api.requests import GetDatasetTypes
from movici_api_client.cli.common import Controller
from movici_api_client.cli.decorators import authenticated, command, format_output
from movici_api_client.cli.dependencies import get


class DatasetTypeController(Controller):
    name = "dataset_type"
    decorators = (authenticated, format_output)

    @command(name="dataset_types", group="get")
    def list(self):
        client = get(Client)
        return client.request(GetDatasetTypes())
