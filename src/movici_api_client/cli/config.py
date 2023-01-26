from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import typing as t

import gimme

from movici_api_client.cli.helpers import read_json_file

from .exceptions import DuplicateContext, InvalidConfigFile, InvalidFile

# "~" is properly expanded using pathlib.Path.expanduser() on all platforms including windows
DEFAULT_CONFIG_LOCATION = "~/.movici.conf"
CONFIG_LOCATION_ENV = "MOVICI_CLI_CONFIG"


def get_config_path(env=CONFIG_LOCATION_ENV, default=DEFAULT_CONFIG_LOCATION):
    return pathlib.Path(os.getenv(env, default=default)).expanduser()


def get_config(file: pathlib.Path = None):
    file = pathlib.Path(file) if file is not None else get_config_path()
    try:
        if not file.is_file():
            return initialize_config(file)

        return Config.from_dict(read_json_file(file))

    except IOError:
        raise InvalidConfigFile("read error", file)
    except InvalidFile as e:
        raise InvalidConfigFile(e.msg, e.file)
    except (KeyError, TypeError):
        raise InvalidConfigFile("invalid values", file)
    except Exception:
        raise InvalidConfigFile("unknown cause", file)


def initialize_config(file: pathlib.Path):
    config = Config.from_dict({"version": 1, "current_context": None, "contexts": []})
    write_config(config, file)
    return config


def read_config(file: pathlib.Path) -> Config:
    return Config.from_dict(json.loads(file.read_text()))


def write_config(config: Config = None, file: t.Optional[pathlib.Path] = None):
    config = config or gimme.that(Config)
    file = pathlib.Path(file) if file is not None else get_config_path()
    file.write_text(json.dumps(config.as_dict(), indent=2))


class Config:
    """An in memory representation of the config file"""

    def __init__(
        self, contexts: t.Sequence[Context], current_context: t.Optional[str] = None, version=1
    ) -> None:
        self.version = version
        self.contexts = list(contexts)
        self.current_context = self.get_context(current_context)

    def activate_context(self, name):
        context = self.get_context(name)
        if not context:
            raise ValueError(f"Invalid config name {name}")
        self.current_context = context

    def get_context(self, name) -> t.Optional[Context]:
        for context in self.contexts:
            if context.name == name:
                return context

    def add_context(self, context: Context):
        if self.get_context(context.name) is not None:
            raise DuplicateContext({context.name})
        self.contexts.append(context)

    def remove_context(self, item: t.Union[str, Context]):
        try:
            if isinstance(item, str):
                name = item
                self.contexts.remove(self.get_context(name))
            else:
                name = item.name
                self.contexts.remove(item)
        except (KeyError, ValueError):
            pass
        else:
            if self.current_context is not None and name == self.current_context.name:
                self.current_context = None

    def as_dict(self):
        return {
            "version": self.version,
            "current_context": self.current_context.name
            if self.current_context is not None
            else None,
            "contexts": [context.as_dict() for context in self.contexts],
        }

    @classmethod
    def from_dict(cls, config: dict):
        return cls(
            current_context=config["current_context"],
            contexts=[Context(**context) for context in config["contexts"]],
            version=config["version"],
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return (
            self.version == other.version
            and self.current_context == other.current_context
            and self.contexts == other.contexts
        )


def parse_bool(value, allow_none=False):
    falsy = {"False", "f", "false"}
    if value is None and allow_none:
        return None
    if value in falsy:
        return False
    return bool(value)


_MISSING = object()


@dataclasses.dataclass
class SpecialKey:
    parse: t.Optional[callable] = None
    default: t.Any = _MISSING
    required: bool = False

    def __post_init__(self):
        self.name = None

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        try:
            return dict.__getitem__(instance, self.name)
        except KeyError:
            if self.default is not _MISSING:
                return self.default
            raise AttributeError

    def __set__(self, instance, value):
        if self.parse:
            value = self.parse(value)
        dict.__setitem__(instance, self.name, value)

    def __del__(self, instance):
        if self.required:
            raise ValueError("Cannot detele required key")
        dict.__delitem__(instance, self.name)

    def __set_name__(self, owner, name):
        self.name = name


def special_key_dict(cls):
    cls.__special_keys__ = {key for key in dir(cls) if isinstance(getattr(cls, key), SpecialKey)}

    def special_keys_func(name, lookup_func):
        def func(self, key, *args, **kwargs):
            if key in self.__special_keys__:
                return lookup_func(self, key, *args, **kwargs)
            if parent_func := getattr(super(cls, self), name, None):
                return parent_func(key, *args, **kwargs)

        func.__name__ = name
        return func

    def get(self, key, default=None):
        if key not in self.__special_keys__:
            return getattr(super(cls, self), "get")(key, default)
        try:
            return getattr(self, key)
        except AttributeError:
            return default

    cls.__getitem__ = special_keys_func("__getitem__", getattr)
    cls.__setitem__ = special_keys_func("__setitem__", setattr)
    cls.__delitem__ = special_keys_func("__delitem__", delattr)
    cls.get = get

    return cls


@special_key_dict
class Context(dict):
    name = SpecialKey(required=True)
    auth = SpecialKey(parse=parse_bool, default=True)
    location = SpecialKey(required=True)
    __special_keys__: t.Set[str]

    def __init__(self, name: str, location: str, **kwargs) -> None:
        super().__init__(name=name, location=location, **kwargs)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Context):
            return False
        return super().__eq__(__o)

    def as_dict(self):
        return dict(self)
