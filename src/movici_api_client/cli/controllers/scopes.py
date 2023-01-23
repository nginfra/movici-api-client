from movici_api_client.cli.events.authorization import CreateScope, DeleteScope, GetScopes

from ..common import Controller
from ..decorators import argument, authenticated, command, format_output, handle_event


class ScopeController(Controller):
    name = "scope"

    decorators = (authenticated,)

    @command(name="scopes", group="get")
    @format_output(fields=("uuid", "name"))
    @handle_event
    def list(self):
        return GetScopes()

    @command
    @argument("name")
    @format_output
    @handle_event
    def create(self, name):
        return CreateScope(name)

    @command
    @argument("name_or_uuid")
    @format_output
    @handle_event
    def delete(self, name_or_uuid):
        return DeleteScope(name_or_uuid, confirm=True)
