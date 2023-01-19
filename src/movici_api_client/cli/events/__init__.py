import dataclasses

from movici_api_client.api import Request

from ..cqrs import Event


@dataclasses.dataclass
class MakeRequest(Event):
    request: Request
