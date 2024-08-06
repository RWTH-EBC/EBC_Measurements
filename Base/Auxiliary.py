import json


def load_json(file_name: str) -> list | dict:
    """Read a json file"""
    with open(file_name, 'r') as f:
        content = json.load(f)
    return content


def dump_json(content: list | dict, file_name: str):
    """Dump a json file"""
    with open(file_name, 'w') as f:
        json.dump(content, f, indent=4)
