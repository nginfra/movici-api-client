import uuid

import pytest

from movici_api_client.cli.common import OPTIONS_COMMAND, has_options
from movici_api_client.cli.decorators import command
from movici_api_client.cli.utils import iter_commands, maybe_set_flag, validate_uuid


def test_command():
    @command
    def func():
        pass

    assert has_options(func, OPTIONS_COMMAND)


def test_iter_commands():
    class Object:
        @command
        def func(self):
            pass

        @command
        def other_func(self):
            pass

        def not_a_command(self):
            pass

    assert set(key for key, _ in iter_commands(Object())) == {"func", "other_func"}


def test_iter_commands_override():
    class A:
        @command
        def func(self):
            pass

        @command
        def other_func(self):
            pass

        def not_a_command(self):
            pass

    class B(A):
        def func(self):
            """not a command"""

    assert set(key for key, _ in iter_commands(B())) == {"other_func"}


@pytest.mark.parametrize(
    "entry",
    [
        uuid.uuid4(),
        str(uuid.uuid4()),
    ],
)
def test_validate_uuid(entry):
    assert validate_uuid(entry)


def test_invalid_uuid():
    assert not validate_uuid("invalid")


@pytest.mark.parametrize(
    "flag, default_yes, default_no, expected",
    [
        (False, False, False, None),
        (True, False, False, True),
        (False, True, False, True),
        (True, True, False, True),
        (False, False, True, False),
        (True, False, True, True),
    ],
)
def test_set_question_flag(flag, default_yes, default_no, expected):
    assert maybe_set_flag(flag, default_yes, default_no) == expected
