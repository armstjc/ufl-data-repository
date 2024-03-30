"""
# Creation Date: 03/29/2024 09:27 PM EDT
# Last Updated Date: 03/29/2024 09:27 PM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: utils.py
# Purpose: Holds utility functions that are not exclusive
    to one part of this data repository.
###############################################################################
"""

import json
import logging
from os import environ, mkdir
from os.path import expanduser


def format_folder_path(folder_path: str) -> str:
    """
    Reformats a folder path into a folder path that
    python can better understand.

    Parameters
    ----------

    `folder_path` (str, mandatory):
        The folder path you want reformatted.

    Returns
    ----------
    A string that should be a valid file path in python.
    """

    return folder_path.replace("\\", "/").replace("//", "/")


def get_fox_api_key() -> str:
    """ """
    try:
        key = environ["FOX_API_TOKEN"]
        return key
    except Exception as e:
        logging.warning(
            "Could not load in the XFL API token from the environment."
            + f"Full exception: {e}"
        )

    home_dir = format_folder_path(expanduser("~"))
    key_path = f"{home_dir}/.ufl/"
    try:
        mkdir(key_path)
    except FileExistsError:
        logging.info(f"`{key_path}` already exists.")
    except Exception as e:
        logging.warning(f"Unhandled exception {e}")

    try:
        with open(f"{key_path}key.json", "r") as f:
            json_data = json.loads(f.read())

        return json_data["fox_key"]
    except Exception as e:
        raise FileNotFoundError(
            "Could not find a valid FOX Sports API key." +
            f"Full exception: {e}"
        )


if __name__ == "__main__":
    print(get_fox_api_key())
