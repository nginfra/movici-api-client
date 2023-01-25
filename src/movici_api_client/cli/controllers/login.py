from movici_api_client.api import Client
from movici_api_client.api.requests import Login

from ..config import Context
from ..utils import echo, prompt

# While this is in the controllers package, it is not a subclass of Controller.
# it must be instantiated from inside a command


class LoginController:
    def __init__(self, client: Client, context: Context) -> None:
        self.client = client
        self.context = context
        self.success = True

    def login(self, ask_username):
        username = self.context.get("username")
        ask_username = ask_username or username is None

        while True:
            if ask_username:
                username = prompt("Username")
            resp = self.try_login(username)
            if self.success:
                self.handle_success(resp, username)
                break

    def try_login(self, username):
        password = prompt("Password", hide_input=True)
        self.success = True  # Assume success unless it's reset by self.fail
        return self.client.request(Login(username, password), on_error=self.fail)

    def fail(self, *_, **__):
        self.success = False
        echo("Invalid credentials, try again...")
        return False  # stop error propagation in Client

    def handle_success(self, resp, username):
        self.context["auth_token"] = resp["session"]
        self.context["username"] = username
