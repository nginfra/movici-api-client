import dataclasses
import unittest.mock
from collections import deque

from movici_api_client.api.client import Client


class FakeClient(Client):
    def __init__(self, *args, **kwargs) -> None:
        self.responses = deque()
        self.request = unittest.mock.Mock(side_effect=self._request)

    def _request(self, req, on_error=None):
        response = self.next_response()

        if response is None:
            return
        if response.status_code >= 400:
            on_error(response)
        else:
            return req.make_response(response)

    def set_response(self, response_data=None, status_code=200):
        self.responses = deque([FakeResponse(response_data, status_code)])

    def add_response(self, response_data=None, status_code=200):
        self.responses.append(FakeResponse(response_data, status_code))

    def next_response(self):
        try:
            return self.responses.popleft()
        except IndexError:
            return None


@dataclasses.dataclass
class FakeResponse:
    data: dict = None
    status_code: int = 200

    def json(self):
        return self.data
