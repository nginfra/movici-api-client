from __future__ import annotations

import functools
import typing as t

__MOVICI_CLI_OPTIONS__ = "__movici_cli_options__"

OPTIONS_COMMAND = "command"


def set_options(obj, key: str, options: dict):
    opts = getattr(obj, __MOVICI_CLI_OPTIONS__, {})
    opts[key] = {**opts.get(key, {}), **options}
    setattr(obj, __MOVICI_CLI_OPTIONS__, opts)


def get_options(obj, key: str) -> t.Optional[dict]:
    return getattr(obj, __MOVICI_CLI_OPTIONS__, {}).get(key, None)


def has_options(obj, key: str) -> bool:
    return key in getattr(obj, __MOVICI_CLI_OPTIONS__, {})


def remove_options(obj, key: str):
    options: dict
    if options := getattr(obj, __MOVICI_CLI_OPTIONS__, None):
        del options[key]


def command(func=None, /, name=None):
    if func is None:
        return functools.partial(command, name=name)
    set_options(func, OPTIONS_COMMAND, {"name": name})
    return func


def iter_commands(obj: Controller):
    for key in dir(obj):
        val = getattr(obj, key)
        if (opts := get_options(val, OPTIONS_COMMAND)) is not None:
            yield (key, val, opts)


class Controller:
    name: str
