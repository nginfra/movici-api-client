from unittest.mock import patch

import pytest

import movici_api_client.cli.controllers.login
from movici_api_client.cli.config import Context
from movici_api_client.cli.controllers.login import LoginController


@pytest.fixture
def prompt():
    with patch.object(movici_api_client.cli.controllers.login, "prompt") as prompt:
        yield prompt


@pytest.fixture
def context():
    return Context("test-context", "https://example.org")


@pytest.fixture
def controller(prompt, client, context):
    client.set_response({"session": "some_session_token"})
    prompt.side_effect = [
        "user",
        "pw",
    ]
    return LoginController(client, context)


def test_login_calls_client(controller, client):
    controller.login(ask_username=True)
    request = client.request.call_args[0][0]
    assert request.username == "user"
    assert request.password == "pw"


def test_login_prompts_again_after_failure(controller, prompt):
    controller.client.set_response(status_code=401)
    controller.client.add_response({"session": "some_session_token"})
    prompt.side_effect = ["user", "wrong_pw", "user", "right_pw"]
    controller.login(ask_username=True)
    assert prompt.call_count == 4


def test_login_asks_for_username_when_instructed(controller, prompt, context, client):
    context.username = "some_user"
    prompt.side_effect = ["other_user", "password"]
    controller.login(ask_username=True)
    request = client.request.call_args[0][0]
    assert request.username == "other_user"
    assert request.password == "password"


def test_login_writes_info_to_config(prompt, controller, context):
    controller.login(ask_username=True)
    assert context.username == "user"
    assert context.auth_token == "some_session_token"


def test_login_reads_username_from_context(controller, prompt, context, client):
    context.username = "some_user"
    prompt.side_effect = ["wrong_password", "correct_password"]
    controller.client.set_response(status_code=401)
    controller.client.add_response({"session": "some_session_token"})

    controller.login(ask_username=False)

    request = client.request.call_args[0][0]
    assert request.username == "some_user"
    assert request.password == "correct_password"


@pytest.mark.parametrize(
    "user_in_context, ask_username, prompt_username, expected",
    [
        (None, False, "some_user", "some_user"),
        (None, True, "some_user", "some_user"),
        ("other_user", False, "some_user", "other_user"),
        ("other_user", True, "some_user", "some_user"),
    ],
)
def test_login_with_different_username_situations(
    user_in_context, ask_username, prompt_username, expected, controller, prompt
):
    controller.context.username = user_in_context
    prompt.return_value = prompt_username
    prompt.side_effect = None

    controller.login(ask_username)
    request = controller.client.request.call_args[0][0]
    assert request.username == expected
