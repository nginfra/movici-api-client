import click

from .common import Controller, iter_commands


def create_click_command(func, opts):
    return click.command(func)


def register_controller(group: click.Group, controller: Controller):
    for group_name, func, opts in iter_commands(controller):
        command_name = opts.get("name") or controller.name
        if sub_group := group.commands.get(group_name):
            if not isinstance(sub_group, click.Group):
                raise TypeError(
                    f"cannot add commands for controller {controller.name}: "
                    f"{group_name} is not a command group"
                )
            is_new = False
        else:
            sub_group = click.Group(group_name)
            is_new = True
        sub_group.add_command(create_click_command(func, opts), command_name)
        if is_new:
            group.add_command(sub_group)
        
