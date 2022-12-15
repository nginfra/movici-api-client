from movici_api_client.cli.ui import format_table


def test_format_table():
    objs = [
        {"a": 1, "b": 10},
        {"a": 2, "b": 20},
    ]
    print(format_table(objs, keys=("a", "b")))
    assert format_table(objs, keys=("a", "b")) == "\n".join(
        [
            "  a    b",
            "---  ---",
            "  1   10",
            "  2   20",
        ]
    )
