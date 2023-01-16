import functools
import typing as t

from movici_api_client.api import Client, Response
from movici_api_client.api.requests import CheckAuthToken
from movici_api_client.cli.controllers.common import resolve_data_directory

from . import dependencies
from .common import OPTIONS_COMMAND, get_options, has_options, set_options
from .exceptions import InvalidActiveProject, MoviciCLIError, NoActiveProject, Unauthenticated
from .ui import format_anything, format_table
from .utils import DirPath, assert_current_context, echo, get_project_uuids, handle_movici_error


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

        context = assert_current_context()
        if context.get("auth"):
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
        opts["arguments"] = [(args, kwargs), *opts.get("arguments", [])]

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
        result = format_anything(result, fields)
        if header is not None:
            result = header + "\n" + result
        echo(result)

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
        project = context.get("project")
        if not project:
            raise NoActiveProject()

        projects_dict = get_project_uuids()

        try:
            project_uuid = projects_dict[project]
        except KeyError:
            raise InvalidActiveProject(project)

        return func(project_uuid, *args, **kwargs)

    return decorated


def upload_options(func):
    return combine_decorators(
        [
            option("-o", "--overwrite", is_flag=True, help="Always overwrite"),
            option("-c", "--create", is_flag=True, help="Always create if necessary"),
            option(
                "-y",
                "--yes",
                is_flag=True,
                help="Answer yes to all questions, equivalent to -o -c",
            ),
            option("-n", "--no", is_flag=True, help="Answer no to all questions"),
        ]
    )(func)


def download_options(
    purpose: t.Literal["datasets", "scenarios", "updates", "views"],
):
    def decorator(func):
        return combine_decorators(
            [
                option(
                    "-d",
                    "--directory",
                    type=DirPath(writable=True),
                    default=None,
                    callback=lambda _, __, path: resolve_data_directory(path, purpose),
                ),
                option("-o", "-y", "--overwrite", is_flag=True, help="Always overwrite"),
                option("-n", "--no-overwrite", is_flag=True, help="Never overwrite"),
            ]
        )(func)

    return decorator


def combine_decorators(decorators: t.Iterable[callable]):
    def decorator(func):
        return functools.reduce(
            lambda f, decorator: decorator(f),
            decorators,
            func,
        )

    return decorator
