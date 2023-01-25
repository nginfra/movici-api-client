from movici_api_client.cli.common import Controller
from movici_api_client.cli.decorators import authenticated, command, format_output, handle_event
from movici_api_client.cli.events.dataset import GetDatasetTypes


class DatasetTypeController(Controller):
    name = "dataset_type"
    decorators = (authenticated, format_output)

    @command(name="dataset_types", group="get")
    @format_output
    @handle_event
    def list(self):
        return GetDatasetTypes()
