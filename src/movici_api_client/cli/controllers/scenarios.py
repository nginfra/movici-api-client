import asyncio
import json
import pathlib
import time

from movici_api_client.api.client import Client
from movici_api_client.api.requests import (
    CreateScenario,
    DeleteScenario,
    DeleteSimulation,
    DeleteTimeline,
    GetScenarios,
    GetSimulation,
    GetSingleScenario,
    RunSimulation,
    UpdateScenario,
)

from ..common import Controller
from ..decorators import (
    argument,
    authenticated,
    combine_decorators,
    command,
    download_options,
    format_output,
    option,
    upload_options,
    valid_project_uuid,
)
from ..dependencies import get
from ..exceptions import InvalidUsage
from ..filetransfer import (
    DownloadScenarios,
    DownloadSingleScenario,
    ScenarioUploadStrategy,
    UploadMultipleResources,
    UploadScenario,
    resolve_question_flag,
)
from ..helpers import edit_resource
from ..utils import Choice, DirPath, FilePath, confirm, echo, maybe_set_flag, prompt
from .common import get_scenario, get_scenario_uuid, resolve_data_directory


def upload_scenario_options(func):
    return combine_decorators(
        [
            option(
                "-i",
                "--inspect",
                is_flag=True,
                help=(
                    "Inspect the file to ensure the name inside the payload matches the filename"
                ),
            ),
            option(
                "--with-simulation",
                is_flag=True,
                help=("Also upload local simulation results (if they exist)"),
            ),
            option(
                "--with-views",
                is_flag=True,
                help=("Also upload local views (if they exist)"),
            ),
        ]
    )(upload_options(func))


class ScenarioController(Controller):
    name = "scenario"
    decorators = (valid_project_uuid, authenticated)

    @command(name="scenarios", group="get")
    @format_output
    def list(self, project_uuid):
        return self.client.request(GetScenarios(project_uuid))

    @command
    @argument("name_or_uuid")
    @option("-o", "--output", type=Choice(["json"], case_sensitive=False), help="output format")
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
    def get(self, project_uuid, name_or_uuid, output):
        uuid = get_scenario_uuid(name_or_uuid, project_uuid, client=self.client)

        result = self.client.request(GetSingleScenario(uuid))
        if output == "json":
            result = json.dumps(result, indent=2)
        return result

    @command
    @argument("name", default="")
    @option("--display-name")
    @option("--description")
    @format_output
    def create(self, project_uuid, name, display_name, description):
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
        return self.client.request(CreateScenario(project_uuid, payload=payload))

    @command
    @argument("name_or_uuid")
    @format_output
    def delete(self, project_uuid, name_or_uuid):
        uuid = get_scenario_uuid(name_or_uuid, project_uuid, client=self.client)

        confirm(
            f"Are you sure you wish to delete scenario '{name_or_uuid}' and all associated data?"
        )
        return self.client.request(DeleteScenario(uuid))

    @command
    @argument("name_or_uuid")
    def clear(self, project_uuid, name_or_uuid):
        def on_error(resp):
            return resp.status_code != 404

        uuid = get_scenario_uuid(name_or_uuid, project_uuid, client=self.client)

        confirm(
            f"Are you sure you wish to clear scenario '{name_or_uuid}' of its simulation results?"
        )
        self.client.request(DeleteTimeline(uuid), on_error=on_error)
        self.client.request(DeleteSimulation(uuid), on_error=on_error)
        wait_until_simulation_is_reset(uuid, client=self.client)
        echo("Scenario successfully cleared!")

    @command
    @argument("name_or_uuid")
    @option("-o", "-y", "--overwrite", is_flag=True, help="Always overwrite")
    @option("-n", "--no-overwrite", is_flag=True, help="Never overwrite")
    def run(self, project_uuid, name_or_uuid, overwrite, no_overwrite):
        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")

        scenario = get_scenario(name_or_uuid, project_uuid, self.client)
        uuid, has_timeline = scenario["uuid"], scenario["has_timeline"]
        overwrite = maybe_set_flag(False, overwrite, no_overwrite)
        if has_timeline:
            do_overwrite = resolve_question_flag(
                overwrite,
                (
                    f"Scenario {name_or_uuid} already has simulation results, "
                    "do you wish to overwrite?",
                ),
            )
            if not do_overwrite:
                echo(f"Cowardly refusing to overwrite simulation results for '{name_or_uuid}'")
                return

            def on_error(resp):
                return resp.status_code != 404

            self.client.request(DeleteTimeline(uuid), on_error=on_error)
            self.client.request(DeleteSimulation(uuid), on_error=on_error)
            wait_until_simulation_is_reset(uuid, client=self.client)
        self.client.request(RunSimulation(uuid))
        echo("Simulation started!")

    @command
    @argument("name_or_uuid", default="")
    @option("-f", "--file", type=FilePath(), required=True)
    @upload_scenario_options
    def upload(
        self,
        project_uuid,
        name_or_uuid,
        file: pathlib.Path,
        overwrite,
        create,
        yes,
        no,
        inspect,
        **kwargs,
    ):

        asyncio.run(
            UploadScenario(
                client=self.client,
                file=file,
                parent_uuid=project_uuid,
                name_or_uuid=name_or_uuid,
                overwrite=maybe_set_flag(overwrite, yes, no),
                create_new=maybe_set_flag(create, yes, no),
                inspect_file=inspect,
                **kwargs,
            ).run()
        )

        echo("Success!")

    @command(name="scenarios", group="upload")
    @option(
        "-d",
        "--directory",
        type=DirPath(writable=True),
        default=None,
        callback=lambda _, __, path: resolve_data_directory(path, "scenarios"),
    )
    @upload_scenario_options
    def upload_multiple(
        self,
        project_uuid,
        directory,
        overwrite,
        create,
        yes,
        no,
        inspect,
        with_simulation,
        with_views,
    ):
        if yes and no:
            raise InvalidUsage("cannot combine --yes with --no")
        self.params.overwrite = maybe_set_flag(overwrite, yes, no)
        self.params.create = maybe_set_flag(create, yes, no)
        self.params.inspect = inspect
        self.params.with_simulation = with_simulation
        self.params.with_views = with_views
        strategy = ScenarioUploadStrategy(client=self.async_client)

        asyncio.run(
            UploadMultipleResources(
                directory,
                project_uuid,
                strategy=strategy,
                upload_task=UploadScenario,
            ).run()
        )

    @command
    @argument("name_or_uuid")
    @download_options(purpose="scenarios")
    @option("--with-simulation", is_flag=True)
    @option("--with-views", is_flag=True)
    def download(
        self,
        project_uuid,
        name_or_uuid,
        directory,
        overwrite,
        no_overwrite,
        with_simulation,
        with_views,
    ):
        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")
        self.params.overwrite = maybe_set_flag(False, overwrite, no_overwrite)
        self.params.with_simulation = with_simulation
        self.params.with_views = with_views

        scenario = get_scenario(name_or_uuid, project_uuid, client=self.client)
        asyncio.run(
            DownloadSingleScenario(
                parent=scenario,
                directory=directory,
                overwrite=maybe_set_flag(False, default_yes=overwrite, default_no=no_overwrite),
            ).run()
        )
        echo("Success!")

    @command(name="scenarios", group="download")
    @download_options(purpose="scenarios")
    @option("--with-simulation", is_flag=True)
    @option("--with-views", is_flag=True)
    def download_multiple(self, project_uuid, directory, overwrite, no_overwrite, **kwargs):
        if overwrite and no_overwrite:
            raise InvalidUsage("cannot combine --overwrite with --no-overwrite")
        asyncio.run(
            DownloadScenarios(
                {"uuid": project_uuid},
                directory=directory,
                overwrite=maybe_set_flag(False, default_yes=overwrite, default_no=no_overwrite),
                progress=False,
                cli_params=kwargs,
            ).run()
        )
        echo("Success!")

    @command
    @argument("name_or_uuid")
    def edit(self, project_uuid, name_or_uuid):
        uuid = get_scenario_uuid(name_or_uuid, project_uuid, client=self.client)
        current = self.client.request(GetSingleScenario(uuid))
        result = edit_resource(current)
        self.client.request(UpdateScenario(uuid, result))
        echo("Succesfully updated scenario")


def wait_until_simulation_is_reset(uuid, interval=1, client=None):
    client = client or get(Client)
    has_simulation = True

    def on_error(resp):
        nonlocal has_simulation
        if resp.status_code == 404:
            has_simulation = False
            return False

    while has_simulation:
        client.request(GetSimulation(uuid), on_error=on_error)
        time.sleep(interval)
