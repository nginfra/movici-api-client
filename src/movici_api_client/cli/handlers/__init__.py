import typing as t

from ..cqrs import EventHandler
from .remote import ALL_HANDLERS as REMOTE_HANDLERS  # noqa


def get_handlers_dict(handlers: t.Sequence[EventHandler]):
    return {h.__event__: h for h in handlers}
