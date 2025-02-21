import json
from datetime import datetime
from typing import Any, Dict


def json2str(obj: Any) -> str:
    """
    Convert an object to a JSON string.

    Args:
        obj (Any): The object to convert.

    Returns:
        str: JSON string representation of the object.
    """
    return json.dumps(
        to_dict(obj), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )


def str2json(json_str: str) -> Dict:
    """
    Convert a JSON string to a dictionary.

    Args:
        json_str (str): JSON string to convert.

    Returns:
        Dict: Dictionary representation of the JSON string.
    """
    return json.loads(json_str)


def to_dict(obj: Any, classkey: str = None) -> Any:
    """
    Recursively convert an object to a dictionary.

    Args:
        obj (Any): The object to convert.
        classkey (str, optional): Key to store class name if applicable. Defaults to None.

    Returns:
        Any: Dictionary representation of the object.
    """
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, dict):
        return {k: to_dict(v, classkey) for k, v in obj.items()}
    if hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    if hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v, classkey) for v in obj]
    if hasattr(obj, "__dict__"):
        data = {
            key: to_dict(value, classkey)
            for key, value in obj.__dict__.items()
            if not callable(value) and not key.startswith("_")
        }
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    return obj