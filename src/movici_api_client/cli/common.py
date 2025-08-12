from __future__ import annotations

import dataclasses
import typing as t

import gimme

from movici_api_client.api import AsyncClient, Client
from movici_api_client.cli.cqrs import Mediator

__MOVICI_CLI_OPTIONS__ = "__movici_cli_options__"

OPTIONS_COMMAND = "command"


def set_options(obj, key: str, options: dict):
    opts = getattr(obj, __MOVICI_CLI_OPTIONS__, {})
    opts[key] = {**opts.get(key, {}), **options}
    setattr(obj, __MOVICI_CLI_OPTIONS__, opts)


def get_options(obj, key: str) -> dict | None:
    return getattr(obj, __MOVICI_CLI_OPTIONS__, {}).get(key, None)


def has_options(obj, key: str) -> bool:
    return key in getattr(obj, __MOVICI_CLI_OPTIONS__, {})


def remove_options(obj, key: str):
    options: dict
    if options := getattr(obj, __MOVICI_CLI_OPTIONS__, None):
        del options[key]


@dataclasses.dataclass
class CLIParameters:
    overwrite: bool | None = None
    no_overwrite: bool | None = None
    create: bool | None = None
    inspect: bool | None = None
    yes: bool | None = None
    no: bool | None = None
    with_simulation: bool | None = None
    with_views: bool | None = None
    output: str | None = None


class Controller:
    name: str
    reverse: bool = True
    decorators: t.Iterable[callable] = ()
    __commands__: set[callable]

    mediator: Mediator = gimme.attribute(Mediator)
    client: Client = gimme.attribute(Client)
    async_client: AsyncClient = gimme.attribute(AsyncClient)
    params: CLIParameters = gimme.attribute(CLIParameters)

    def __init_subclass__(cls) -> None:
        __commands__ = set()
        for key in dir(cls):
            val = getattr(cls, key)
            if get_options(val, OPTIONS_COMMAND):
                __commands__.add(key)
        cls.__commands__ = __commands__
