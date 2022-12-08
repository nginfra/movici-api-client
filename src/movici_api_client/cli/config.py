from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import typing as t

from .exceptions import InvalidConfig

# "~" is properly expanded using pathlib.Path.expanduser() on all platforms including windows
DEFAULT_CONFIG_LOCATION = "~/.movici.conf"
CONFIG_LOCATION_ENV = "MOVICI_CLI_CONFIG"


def get_config_path(env=CONFIG_LOCATION_ENV, default=DEFAULT_CONFIG_LOCATION):
    return pathlib.Path(os.getenv(env, default=default)).expanduser()


def get_config(file: pathlib.Path = None):
    file = pathlib.Path(file) if file is not None else get_config_path()
    try:
        return read_config(file) if file.is_file() else initialize_config(file)

    except IOError:
        raise InvalidConfig("read error", file)
    except json.JSONDecodeError:
        raise InvalidConfig("invalid json", file)
    except (KeyError, TypeError):
        raise InvalidConfig("invalid values", file)
    except:
        raise InvalidConfig("unknown cause", file)


def initialize_config(file: pathlib.Path):
    config = Config.from_dict({"version": 1, "current_context": None, "contexts": []})
    write_config(config, file)
    return config


def read_config(file: pathlib.Path) -> Config:
    return Config.from_dict(json.loads(file.read_text()))


def write_config(config: Config, file: t.Optional[pathlib.Path]=None):
    file = pathlib.Path(file) if file is not None else get_config_path()
    file.write_text(json.dumps(config.asdict(), indent=2))


class Config:
    """An in memory representation of the config file"""

    def __init__(
        self, contexts: t.Sequence[Context], current_context: t.Optional[str] = None, version=1
    ) -> None:
        self.version = version
        self.contexts = list(contexts)
        self._reindex_contexts()
        self._current_context = None
        if current_context in self.contexts_by_name:
            self._current_context = current_context

    @property
    def current_context(self) -> t.Optional[Context]:
        if self._current_context is None:
            return None
        return self.contexts[self.contexts_by_name[self._current_context]]

    def _reindex_contexts(self):
        self.contexts_by_name = {c.name: idx for idx, c in enumerate(self.contexts)}

    def activate_context(self, name):
        if name not in self.contexts_by_name:
            raise ValueError(f"Invalid config name {name}")
        self._current_context = name

    def get_context(self, name) -> Context:
        return self.contexts[self.contexts_by_name[name]]

    def add_context(self, context: Context):
        if context.name in self.contexts_by_name:
            raise ValueError(f"Config {context.name} already exists")
        self.contexts.append(context)
        self._reindex_contexts()

    def remove_context(self, item: t.Union[str, Context]):
        try:
            if isinstance(item, str):
                name = item
                idx = self.contexts_by_name[item]
                self.contexts.pop(idx)
            else:
                name = item.name
                self.contexts.remove(item)
        except (KeyError, ValueError):
            pass
        else:
            if name == self._current_context:
                self._current_context = None
        self._reindex_contexts()

    def asdict(self):
        return {
            "version": self.version,
            "current_context": self._current_context,
            "contexts": [dataclasses.asdict(context) for context in self.contexts],
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
            self.version == other.version and
            self._current_context == other._current_context and
            self.contexts == other.contexts
        )

@dataclasses.dataclass
class Context:
    name: str
    url: str
    project: t.Optional[str] = None
    username: t.Optional[str] = None
    auth_token: t.Optional[str] = None


