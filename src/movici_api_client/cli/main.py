import typing as t
from json import JSONDecodeError

import questionary

from movici_api_client.api import Client, Response
from movici_api_client.api.auth import MoviciTokenAuth
from movici_api_client.api.requests import GetProjects, Login
from movici_api_client.cli.exceptions import InvalidProject

from . import dependencies
from .config import Config, get_config, write_config
from .utils import (
    Abort,
    argument,
    assert_context,
    command,
    echo,
    get_project_uuids,
    prompt,
    prompt_choices,
)


def main():
    config = get_config()
    dependencies.set(config)
    if context := config.current_context:
        client = Client(
            base_url=context.url,
            auth=MoviciTokenAuth(auth_token=context.auth_token),
            on_error=handle_http_error,
        )
        dependencies.set(client)


def handle_http_error(resp: Response):
    try:
        msg = resp.json()
    except JSONDecodeError:
        msg = resp.status_code

    echo(f"HTTP Error: {msg}")
    raise Abort()


@command
def login():
    client = dependencies.get(Client)
    config = dependencies.get(Config)
    context = assert_context(config)

    echo(f"Login to {context.url}:")

    success = False

    def fail(*args, **kwargs):
        nonlocal success
        success = False
        echo("Invalid credentials, try again...")
        return False

    while True:
        username, password = prompt_username_and_password()
        success = True
        resp = client.request(Login(username, password), on_error=fail)
        if success:
            context.auth_token = resp["session"]
            write_config(config)
            echo("Success!")
            break


def prompt_username_and_password():
    return prompt("Username"), prompt("Password", hide_input=True)


@command(name="activate-project")
@argument("project", default="")
def activate_project(project):
    config = dependencies.get(Config)
    context = assert_context(config)

    projects_dict = get_project_uuids()
    if not project:
        project = prompt_choices("Choose a project", sorted(projects_dict))
    if project not in projects_dict:
        raise InvalidProject(project)

    context.project = project
    write_config(config)
    echo(f"Project {project} succefully activated!")
