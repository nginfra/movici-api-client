import functools
import pathlib
import typing as t
import uuid

import questionary
from click import Abort
from click import Path as PathType
from click import confirm, echo, prompt

from movici_api_client.api.client import Client
from movici_api_client.api.common import Request
from movici_api_client.api.requests import GetProjects
from movici_api_client.cli import dependencies
from movici_api_client.cli.common import (
    OPTIONS_COMMAND,
    Controller,
    get_options,
)
from movici_api_client.cli.config import Config
from movici_api_client.cli.exceptions import (
    InvalidResource,
    MoviciCLIError,
    NoActiveProject,
    NoConfig,
    NoCurrentContext,
)

# Show static analysis tools that we're using these imports with the intent to export, proxy and
# possibly adapt them
confirm = confirm
prompt = prompt
echo = echo
Abort = Abort
confirm = confirm

DirPath = functools.partial(
    PathType, file_okay=False, readable=True, exists=True, path_type=pathlib.Path
)
FilePath = functools.partial(
    PathType, dir_okay=False, readable=True, exists=True, path_type=pathlib.Path
)


def assert_context(config: Config):
    if config is None:
        raise NoConfig()
    if (rv := config.current_context) is None:
        raise NoCurrentContext()
    return rv


def assert_current_context():
    config = dependencies.get(Config)
    return assert_context(config)


def assert_active_project(project=None):
    if project is None:
        context = assert_current_context()
        if context.project is None:
            raise NoActiveProject()
        project = context.project
    return assert_project_uuid(project)


def assert_resource_uuid(resource: str, request: Request, resource_type='resource'):
    resources = get_resource_uuids(request)
    try:
        return resources[resource]
    except KeyError:
        raise InvalidResource(resource_type, resource)


def assert_project_uuid(project: str):
    return assert_resource_uuid(project, GetProjects(), resource_type="project")


def get_resource_uuids(request: Request):
    client = dependencies.get(Client)
    all_resources = client.request(request)
    return {p["name"]: p["uuid"] for p in all_resources}


def get_project_uuids():
    return get_resource_uuids(GetProjects())


def handle_movici_error(e: MoviciCLIError):
    echo(str(e), err=True)
    raise Abort()


def iter_commands(obj: Controller):
    for key in dir(obj):
        val = getattr(obj, key)
        if opts := get_options(val, OPTIONS_COMMAND):
            group_name = opts.get("group_name") or key
            yield (group_name, val)


def prompt_choices(question: str, choices: t.Sequence[str]):
    return questionary.select(
        question,
        choices=choices,
        use_shortcuts=len(choices) < 36,
        use_arrow_keys=True,
    ).unsafe_ask()


def validate_uuid(entry: t.Union[str, uuid.UUID]):
    if isinstance(entry, uuid.UUID):
        return True
    try:
        uuid.UUID(entry)
    except ValueError:
        return False

    return True
