import dataclasses

from movici_api_client.cli.cqrs import Event


@dataclasses.dataclass
class GetScopes(Event):
    pass


@dataclasses.dataclass
class CreateScope(Event):
    name: str


@dataclasses.dataclass
class DeleteScope(Event):
    name_or_uuid: str
    confirm: bool
