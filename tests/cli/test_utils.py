import uuid
from movici_api_client.cli.common import OPTIONS_COMMAND, has_options
from movici_api_client.cli.utils import command, iter_commands, validate_uuid


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


def test_validate_uuid():
    assert validate_uuid(uuid.uuid4())

def test_invalid_uuid():
    assert validate_uuid("invalid")