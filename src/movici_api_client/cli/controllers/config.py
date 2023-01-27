import pathlib

import gimme

from movici_api_client.cli.config import Config, Context, write_config
from movici_api_client.cli.exceptions import Conflict, NoContextAvailable, NotFound

from ..common import Controller
from ..decorators import argument, command, format_output, option
from ..utils import assert_context, confirm, echo, prompt


class ConfigController(Controller):
    name = "config"
    reverse = False
    config: Config = gimme.attribute(Config)

    @command
    @argument("name")
    def create(self, name):
        config = self.config
        if config.get_context(name):
            raise Conflict("Context", name)

        echo("Creating a new context, please give the following information")

        is_local = confirm("Is this a local context?", abort=False)
        location = get_local_location() if is_local else get_remote_location()
        context = Context(name, location, local=is_local)
        if is_local:
            context["auth"] = False
        config.add_context(context)

        activate = confirm("Do you wish to activate this context?", default=True)
        if activate:
            config.activate_context(name)
        write_config(config)
        echo("Context succesfully created")

    @command
    @argument("context")
    def activate(self, context: str):
        config = self.config
        if not len(config.contexts):
            raise NoContextAvailable()
        try:
            config.activate_context(context)
        except ValueError:
            raise NotFound("Context", context)

        write_config(config)

    @command
    @argument("context")
    def delete(self, context: str):
        self.config.delete_context(context)
        echo("Context succesfully deleted")

    @command
    @option("-a", "--all", is_flag=True)
    @format_output
    def show(self, all):
        config = self.config
        if all:
            return config.contexts
        else:
            return assert_context(config)

    @command
    @argument("key")
    @argument("value")
    def set(self, key, value):
        config = self.config
        context = assert_context(config)
        context[key] = value

        write_config(config)
        echo("Context succesfully updated")

    @command
    @argument("keys", nargs=-1, required=True)
    def unset(self, keys):

        config = self.config
        context = assert_context(config)

        for key in keys:
            try:
                del context[key]
            except KeyError:
                pass
            except ValueError:
                echo(f"Cannot unset read-only field '{key}'", err=True)

        write_config(config)
        echo("Context succesfully updated")


def get_remote_location():
    return prompt("Base URL (eg: https://example.org/)")


def get_local_location():
    result = prompt("Path (eg: ~/movici)")
    return str(pathlib.Path(result).resolve())
