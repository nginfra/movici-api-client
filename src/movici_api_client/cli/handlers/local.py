import itertools
import typing as t

from movici_api_client.cli.events.project import (
    CreateProject,
    DeleteProject,
    DownloadProject,
    EditProject,
    GetAllProjects,
    GetSingleProject,
    UploadProject,
)
from movici_api_client.cli.exceptions import InvalidUsage

from ..cqrs import Event, EventHandler, Mediator


class LocalProjectsHandler(EventHandler):
    __event__ = (
        GetAllProjects,
        GetSingleProject,
        CreateProject,
        UploadProject,
        DeleteProject,
        UploadProject,
        DownloadProject,
        EditProject,
    )

    async def handle(self, event: Event, mediator: Mediator):
        raise InvalidUsage("Local contexts do no support projects")


def parse_handler(handler: t.Type[EventHandler]):
    if isinstance(handler.__event__, (tuple, list)):
        yield from ((e, handler) for e in handler.__event__)
    else:
        yield (handler.__event__, handler)


ALL_HANDLERS = dict(
    itertools.chain.from_iterable(
        parse_handler(obj)
        for obj in globals().values()
        if isinstance(obj, type) and obj is not EventHandler and issubclass(obj, EventHandler)
    )
)
