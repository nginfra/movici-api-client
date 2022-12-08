import functools
import typing as t

import questionary
from click import Abort, confirm, echo, prompt

from movici_api_client.api.client import Client, Response
from movici_api_client.api.requests import CheckAuthToken, GetProjects
from movici_api_client.cli import dependencies
from movici_api_client.cli.common import (
    OPTIONS_COMMAND,
    Controller,
    get_options,
    has_options,
    set_options,
)
from movici_api_client.cli.config import Config
from movici_api_client.cli.exceptions import (
    InvalidProject,
    MoviciCLIError,
    NoActiveProject,
    NoConfig,
    NoCurrentContext,
    Unauthenticated,
)

# Show static analysis tools that we're using these imports with the intent to export, proxy and
# possibly adapt them
prompt = prompt
echo = echo
Abort = Abort
confirm = confirm


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

def assert_project_uuid(project: str):
    projects_dict = get_project_uuids()
    try:
        return projects_dict[project]
    except KeyError:
        raise InvalidProject(project)


def get_project_uuids():
    client = dependencies.get(Client)
    all_projects = client.request(GetProjects())
    return {p["name"]: p["uuid"] for p in all_projects["projects"]}


def catch_exceptions(func):
    """Decorator for catching (movici) exceptions, and handling them properly"""

    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except MoviciCLIError as e:
            handle_movici_error(e)

    return _decorated


def handle_movici_error(e: MoviciCLIError):
    echo(str(e), err=True)
    raise Abort()


def authenticated(func):
    def on_error(resp: Response):
        if resp.status_code == 401:
            raise Unauthenticated()

    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        client = dependencies.get(Client)
        client.request(CheckAuthToken(), on_error=on_error)
        func(*args, **kwargs)

    return _decorated


def command(func=None, /, name=None):
    if func is None:
        return functools.partial(command, name=name)
    set_options(func, OPTIONS_COMMAND, {"name": name})
    return func


def argument(*args, **kwargs):
    def wrapper(func):
        if not has_options(func, OPTIONS_COMMAND):
            set_options(func, OPTIONS_COMMAND, {})
        opts = get_options(func, OPTIONS_COMMAND)

        opts.setdefault("arguments", []).append((args, kwargs))

        return func

    return wrapper


def option(*args, **kwargs):
    def wrapper(func):
        if not has_options(func, OPTIONS_COMMAND):
            set_options(func, OPTIONS_COMMAND, {})
        opts = get_options(func, OPTIONS_COMMAND)

        opts.setdefault("options", []).append((args, kwargs))

        return func

    return wrapper


def iter_commands(obj: Controller):
    for key in dir(obj):
        val = getattr(obj, key)
        if has_options(val, OPTIONS_COMMAND):
            yield (key, val)


def prompt_choices(question: str, choices: t.Sequence[str]):
    return questionary.select(
        question,
        choices=choices,
        use_shortcuts=len(choices) < 36,
        use_arrow_keys=True,
    ).unsafe_ask()
