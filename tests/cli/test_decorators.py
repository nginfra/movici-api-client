from unittest.mock import call, patch

import pytest

from movici_api_client.api.requests import CheckAuthToken
from movici_api_client.cli import decorators
from movici_api_client.cli.decorators import authenticated, catch_exceptions
from movici_api_client.cli.exceptions import MoviciCLIError, Unauthenticated
from movici_api_client.cli.testing import FakeClient


class TestCatchExceptions:
    def test_catch_exceptions_passes_result(self):
        @catch_exceptions
        def test_func():
            return 42

        assert test_func() == 42

    def test_catch_exceptions_handles_errors(self):
        exc = MoviciCLIError()

        @catch_exceptions
        def test_func():
            raise exc

        with patch.object(decorators, "handle_movici_error") as handle_movici_error:
            test_func()
        assert handle_movici_error.call_args == call(exc)


class TestAuthenticated:
    def test_authenticated_requests_authentication(self, client: FakeClient):
        @authenticated
        def test_func():
            return 42

        test_func()
        assert isinstance(client.request.call_args[0][0], CheckAuthToken)

    def test_authenticated_passes_when_authenticated(self, client: FakeClient):
        @authenticated
        def test_func():
            return 42

        assert test_func() == 42

    def test_authenticated_raises_when_not_authenticated(self, client: FakeClient):
        client.set_response(status_code=401)

        @authenticated
        def test_func():
            return 42

        with pytest.raises(Unauthenticated):
            test_func()
