import asyncio
import json
import pathlib

from movici_api_client.cli.events.scenario import (
    ClearScenario,
    CreateScenario,
    DeleteScenario,
    DownloadMultipleScenarios,
    DownloadScenario,
    EditScenario,
    GetAllScenarios,
    GetSingleScenario,
    RunSimulation,
    UploadMultipleScenarios,
    UploadScenario,
)

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    cli_options,
    command,
    data_directory_option,
    format_output,
    handle_event,
    option,
)
from ..utils import FilePath, prompt


class ScenarioController(Controller):
    name = "scenario"
    decorators = (authenticated,)

    @command(name="scenarios", group="get")
    @format_output
    @handle_event
    def list(self):
        return GetAllScenarios()

    @command
    @argument("name_or_uuid")
    @cli_options("output")
    @format_output(
        fields=(
            "uuid",
            "name",
            "display_name",
            "description",
            "bounding_box",
            "epsg_code",
            "created_on",
            "last_modified",
            "status",
            "has_timeline",
        )
    )
    def get(self, name_or_uuid):
        result = asyncio.run(self.mediator.send(GetSingleScenario(name_or_uuid=name_or_uuid)))
        if self.params.output == "json":
            result = json.dumps(result, indent=2)
        return result

    @command
    @argument("name", default="")
    @option("--display-name")
    @option("--description")
    @format_output
    @handle_event
    def create(self, name, display_name, description):
        name = name if name != "" else prompt("name")

        if display_name is None:
            display_name = prompt("display name", default="same as name")
        if display_name == "same as name":
            display_name = name

        description = description if description is not None else prompt("description", default="")

        payload = {
            "name": name,
            "display_name": name if display_name == "same as name" else display_name,
            "description": description,
            "version": 4,
            "models": [],
            "datasets": [],
        }
        return CreateScenario(payload)

    @command
    @argument("name_or_uuid")
    @format_output
    @handle_event
    def delete(self, name_or_uuid):
        return DeleteScenario(name_or_uuid)

    @command
    @argument("name_or_uuid")
    @cli_options("yes", "no")
    @handle_event(success_message="Scenario successfully cleared!")
    def clear(self, name_or_uuid):
        return ClearScenario(name_or_uuid=name_or_uuid, confirm=True)

    @command
    @argument("name_or_uuid")
    @cli_options("overwrite", "no_overwrite")
    @handle_event(success_message="Simulation started!")
    def run(self, name_or_uuid):
        return RunSimulation(name_or_uuid)

    @command
    @argument("name_or_uuid", default="")
    @option("-f", "--file", type=FilePath(), required=True)
    @cli_options("overwrite", "create", "yes", "no", "inspect", "with_simulation", "with_views")
    @handle_event(success_message="Success!")
    def upload(self, name_or_uuid, file: pathlib.Path):
        return UploadScenario(name_or_uuid, file)

    @command(name="scenarios", group="upload")
    @data_directory_option(purpose="scenarios")
    @cli_options("overwrite", "create", "yes", "no", "inspect", "with_simulation", "with_views")
    @handle_event(success_message="Success!")
    def upload_multiple(self, directory):
        return UploadMultipleScenarios(directory)

    @command
    @argument("name_or_uuid")
    @data_directory_option(purpose="scenarios")
    @cli_options("overwrite", "yes", "no", "with_simulation", "with_views")
    @handle_event(success_message="Success!")
    def download(self, name_or_uuid, directory):
        return DownloadScenario(name_or_uuid, directory)

    @command(name="scenarios", group="download")
    @data_directory_option(purpose="scenarios")
    @cli_options("overwrite", "yes", "no", "with_simulation", "with_views")
    @handle_event(success_message="Success!")
    def download_multiple(self, directory):
        return DownloadMultipleScenarios(directory=directory)

    @command
    @argument("name_or_uuid")
    @handle_event(success_message="Succesfully updated scenario")
    def edit(self, name_or_uuid):
        return EditScenario(name_or_uuid)
