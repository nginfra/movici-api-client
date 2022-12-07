from .common import command, Controller


class ProjectController(Controller):
    name = "project"

    @command(name="projects")
    def list(self):
        ...

    @command
    def get(self):
        ...

    @command
    def create(self):
        ...

    @command
    def update(self):
        ...

    @command
    def delete(self):
        ...
