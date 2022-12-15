import dataclasses
import functools

from movici_api_client.api import Client, Response
from movici_api_client.api.requests import CheckAuthToken

from . import dependencies
from .common import OPTIONS_COMMAND, get_options, has_options, set_options
from .exceptions import InvalidActiveProject, MoviciCLIError, NoActiveProject, Unauthenticated
from .ui import format_dataclass, format_dict, format_object, format_table
from .utils import assert_current_context, echo, get_project_uuids, handle_movici_error


def catch_exceptions(func):
    """Decorator for catching (movici) exceptions, and handling them properly"""

    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MoviciCLIError as e:
            handle_movici_error(e)

    return _decorated


def authenticated(func):
    def on_error(resp: Response):
        if resp.status_code == 401:
            raise Unauthenticated()

    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        client = dependencies.get(Client)
        client.request(CheckAuthToken(), on_error=on_error)
        return func(*args, **kwargs)

    return _decorated


def command(func=None, /, name=None, group=None):
    if func is None:
        return functools.partial(command, name=name, group=group)
    set_options(func, OPTIONS_COMMAND, {"name": name, "group_name": group})
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


def format_output(func=None, /, fields=None, header=None):
    if func is None:
        return functools.partial(format_output, fields=fields, header=header)

    @functools.wraps(func)
    def decorated(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, list):
            output = format_table(result, fields)
        elif fields is not None:
            output = format_object(result, fields)
        elif isinstance(result, dict):
            output = format_dict(result)
        elif dataclasses.is_dataclass(result):
            output = format_dataclass(result)

        if header is not None:
            output = header + "\n" + output
        echo(output)

    return decorated


def tabulate_results(func=None, /, keys=()):
    if func is None:
        return functools.partial(tabulate_results, keys=keys)

    @functools.wraps(func)
    def decorated(*args, **kwargs):
        result = func(*args, **kwargs)
        echo(format_table(result, keys))

    return decorated


def valid_project_uuid(func):
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        context = assert_current_context()
        if not context.project:
            raise NoActiveProject()

        projects_dict = get_project_uuids()

        try:
            project_uuid = projects_dict[context.project]
        except KeyError:
            raise InvalidActiveProject(context.project)

        return func(project_uuid, *args, **kwargs)

    return decorated
