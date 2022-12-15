from json import JSONDecodeError

from movici_api_client.api import Client, MoviciTokenAuth, Response
from movici_api_client.cli.exceptions import InvalidResource

from . import dependencies
from .config import Config, get_config, write_config
from .controllers.login import LoginController
from .decorators import argument, authenticated, command, option
from .utils import (
    Abort,
    assert_context,
    assert_current_context,
    echo,
    get_project_uuids,
    prompt_choices,
)


@option("project_override", "-p", "--project", default="")
def main(project_override):
    config = get_config()
    dependencies.set(config)

    if not (context := config.current_context):
        return

    client = Client(
        base_url=context.url,
        auth=MoviciTokenAuth(auth_token=context.auth_token),
        on_error=handle_http_error,
    )
    dependencies.set(client)

    if project_override:
        context.project = project_override


def handle_http_error(resp: Response):
    try:
        msg = resp.json()
    except JSONDecodeError:
        msg = resp.status_code

    echo(f"HTTP Error: {msg}")
    raise Abort()


@command
@option("-U", "--user", "ask_username", is_flag=True, help="always ask for a username")
def login(ask_username):
    client = dependencies.get(Client)
    context = assert_current_context()
    echo(f"Login to {context.url}:")
    LoginController(client, context).login(ask_username)
    write_config()
    echo("Success!")


@command(name="activate-project")
@argument("project", default="")
@authenticated
def activate_project(project):
    config = dependencies.get(Config)
    context = assert_context(config)

    projects_dict = get_project_uuids()
    if not project:
        project = prompt_choices("Choose a project", sorted(projects_dict))
    if project not in projects_dict:
        raise InvalidResource("project", project)

    context.project = project
    write_config(config)
    echo(f"Project {project} succefully activated!")
