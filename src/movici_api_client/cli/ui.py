import dataclasses
import typing as t


def format_object(obj, fields: t.Sequence[str], header=None):
    lines = []
    if header is not None:
        lines.append(header)
    max_field_len = max(len(f) for f in fields)
    for field in fields:
        lines.append(f"{field:<{max_field_len}s}: {get_value(obj, field)!s}")
    return "\n".join(lines)


def format_dataclass(dc, header=None):
    fields = [f.name for f in dataclasses.fields(dc)]
    return format_object(dc, fields, header=header)


def get_value(obj_or_dict, key, default=None):
    if isinstance(obj_or_dict, dict):
        return obj_or_dict.get(key, default)
    else:
        return getattr(obj_or_dict, key, default)
