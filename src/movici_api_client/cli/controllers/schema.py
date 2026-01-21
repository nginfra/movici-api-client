from movici_api_client.cli.common import Controller
from movici_api_client.cli.decorators import (
    authenticated,
    cli_options,
    command,
    data_directory_option,
    format_output,
    handle_event,
)
from movici_api_client.cli.events.dataset import (
    DownloadAttributeSchema,
    GetAttributeSchema,
    GetDatasetTypes,
)


class DatasetTypeController(Controller):
    name = "dataset_type"
    decorators = (authenticated, format_output)

    @command(name="dataset_types", group="get")
    @format_output
    @handle_event
    def list(self):
        return GetDatasetTypes()


class AttributeSchemaController(Controller):
    name = "schema"
    decorators = (authenticated, format_output)

    @command(name="schema", group="get")
    @format_output
    @handle_event
    def list(self):
        return GetAttributeSchema()

    @command
    @data_directory_option(purpose="project")
    @cli_options("overwrite", "yes", "no")
    @handle_event(success_message="Success!")
    def download(self, directory):
        return DownloadAttributeSchema(directory=directory)
