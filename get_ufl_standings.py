"""
# Creation Date: 03/29/2024 06:27 PM EDT
# Last Updated Date: 03/30/2024 12:39 AM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: get_ufl_standings.py
# Purpose: Allows one to get UFL standings data.
###############################################################################
"""

import json
from argparse import ArgumentParser, BooleanOptionalAction
from datetime import UTC, datetime
import logging
from os import mkdir

import pandas as pd
import requests

# from bs4 import BeautifulSoup


def get_ufl_standings(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False,
    save_json: bool = False
):
    """
    Retrieves the current standings from the UFL,
    parses the data.

    Parameters
    ----------

    `season` (int, mandatory):
        Mandatory argument.
        Indicates the season you want UFL standings data from.

    `save_csv` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will save
        the resulting `DataFrame` to a `.csv` file.

    `save_parquet` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will save
        the resulting `DataFrame` to a `.parquet` file.

    `save_json` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will save
        a version of the JSON response as a `.json` file.

    Returns
    ----------
    A pandas `DataFrame` object with UFL standings data.

    """
    # Defaults
    columns_order = [
        "season",
        "week",
        "sr_id",
        "team_uid",
        "team_abbreviation",
        "statbroadcast_id",
        "team_location",
        "team_nickname",
        "conference_name",
        "conference_abv",
        "conference_rank",
        "games_played",
        "wins",
        "losses",
        "ties",
        "win_pct",
        "strength_of_schedule",
        "strength_of_victory",
        "access_time",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36",
        "Referer": "https://www.theufl.com/",
    }
    url = (
        "https://s3.amazonaws.com/s3.statbroadcast.com/hosted/ufl"
        + f"/{season}standings.json"
    )
    now = datetime.now(UTC).isoformat()
    schedule_df = pd.DataFrame()
    schedule_df_arr = []

    try:
        mkdir("standings/current_standings")
    except Exception:
        logging.info("`standings/current_standings` already exists.")

    try:
        mkdir("standings/weekly_standings")
    except Exception:
        logging.info("`standings/weekly_standings` already exists.")

    # Get the JSON file
    response = requests.get(url, headers=headers)

    orj_json_text = response.text
    json_text = orj_json_text[:-1].replace("callback({", "{")

    json_data = json.loads(json_text)
    orj_json_data = json_data
    json_data = json_data["conferences"][0]

    for conference in json_data["divisions"]:
        conference_name = conference["name"]
        conference_abv = conference["alias"]

        temp_df = pd.json_normalize(conference["teams"])

        # NOTE: this may change when there's more games in their API
        temp_df["conference_rank"] = temp_df.index + 1

        temp_df["conference_name"] = conference_name
        temp_df["conference_abv"] = conference_abv
        schedule_df_arr.append(temp_df)

        del temp_df, conference_name, conference_abv

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)

    # Not sure if we *need* to do this,
    # but StatBroadcast has this format for team IDs
    # schedule_df["alias"] = "UFL" + schedule_df["alias"]
    schedule_df["statbroadcast_id"] = "UFL" + schedule_df["alias"]

    schedule_df["access_time"] = now
    schedule_df["games_played"] = (
        schedule_df["wins"] + schedule_df["losses"] + schedule_df["ties"]
    )

    # This way, we have a week attached to standings.
    max_games = int(schedule_df["games_played"].max())
    if max_games == 0:
        max_games += 1

    schedule_df["week"] = max_games
    schedule_df["season"] = season

    schedule_df.rename(
        columns={
            "id": "team_uid",
            "name": "team_nickname",
            "market": "team_location",
            "strength_of_schedule.total": "strength_of_schedule",
            "strength_of_victory.total": "strength_of_victory",
            "alias": "team_abbreviation"
        },
        inplace=True,
    )
    # print(schedule_df)
    # print(schedule_df.dtypes)

    schedule_df = schedule_df[columns_order]

    if save_csv is True:
        schedule_df.to_csv(
            f"standings/current_standings/{season}_ufl_standings.csv",
            index=False
        )
        schedule_df.to_csv(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_standings.csv",
            index=False,
        )

    if save_parquet is True:
        schedule_df.to_parquet(
            f"standings/current_standings/{season}_ufl_standings.parquet",
            index=False
        )
        schedule_df.to_parquet(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_standings.parquet",
            index=False,
        )

    if save_json is True:

        orj_json_data["access_time"] = now
        orj_json_data["season"]["week"] = max_games
        with open(
            f"standings/current_standings/{season}_ufl_standings.json", "w+"
        ) as f:
            f.write(
                json.dumps(
                    orj_json_data,
                    indent=4,
                )
            )

        with open(
            f"standings/weekly_standings/{season}" +
            f"-{max_games:02d}_ufl_standings.json",
            "w+"
        ) as f:
            f.write(
                json.dumps(
                    orj_json_data,
                    indent=4,
                )
            )


if __name__ == "__main__":
    now = datetime.now()

    parser = ArgumentParser()

    parser.add_argument(
        "--save_csv",
        default=False,
        action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_parquet",
        default=False,
        action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_json",
        default=False,
        action=BooleanOptionalAction
    )

    args = parser.parse_args()

    get_ufl_standings(
        season=now.year,
        save_csv=args.save_csv,
        save_parquet=args.save_parquet,
        save_json=args.save_json,
    )
