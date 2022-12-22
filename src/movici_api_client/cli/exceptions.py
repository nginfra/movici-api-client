import dataclasses
import pathlib


@dataclasses.dataclass
class MoviciCLIError(Exception):
    template: str = dataclasses.field(init=False, default=None)

    def __str__(self) -> str:
        if self.template is None:
            return type(self).__name__
        return self.template.format(**dataclasses.asdict(self))


@dataclasses.dataclass
class InvalidFile(MoviciCLIError):
    msg: str
    file: pathlib.Path

    template = "Invalid file [{msg}]: {file!s}"


class InvalidConfigFile(InvalidFile):
    template = "Invalid config file [{msg}]: {file!s}"


class NoCurrentContext(MoviciCLIError):
    template = "No context is activated, please activate a context using `movici config activate`"


class NoConfig(MoviciCLIError):
    template = "No config found"


class Unauthenticated(MoviciCLIError):
    template = "Authentication expired, please login using `movici login`"


@dataclasses.dataclass
class DuplicateContext(MoviciCLIError, ValueError):
    name: str
    template = "Config {name} already exists"


@dataclasses.dataclass
class NoSuchContext(MoviciCLIError):
    context: str
    template = "Context {context} not found"


class NoContextAvailable(MoviciCLIError):
    template = (
        "There are no contexts available. Please create a context using `movici config create`"
    )


@dataclasses.dataclass
class InvalidResource(MoviciCLIError):
    resource_type: str
    name: str
    template = "Invalid {resource_type}: {name}"


@dataclasses.dataclass
class InvalidActiveProject(MoviciCLIError):
    project: str
    template = "Project {project} is not a valid project"


class NoActiveProject(MoviciCLIError):
    template = "No active project"


class NotYetImplemented(MoviciCLIError):
    template = "This command is not implemented yet"


@dataclasses.dataclass
class InvalidUsage(MoviciCLIError):
    msg: str
    template = "Invalid usage: {msg}"
