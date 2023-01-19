import dataclasses

from ..cqrs import Event


@dataclasses.dataclass
class GetAllProjects(Event):
    pass
