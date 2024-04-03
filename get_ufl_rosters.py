"""
# Creation Date: 03/30/2024 10:01 AM EDT
# Last Updated Date: 03/31/2024 03:40 PM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: get_ufl_rosters.py
# Purpose: Allows one to get UFL roster data.
###############################################################################
"""

from argparse import ArgumentParser, BooleanOptionalAction
from datetime import UTC, datetime
import json
import logging
from os import mkdir

import pandas as pd
import requests
from tqdm import tqdm

from utils import get_fox_api_key


def ufl_roster_data(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False
):
    """
    Retrieves roster data from the UFL,
    and parses it into a pandas `DataFrame`.

    Parameters
    ----------

    `season` (int, mandatory):
        Mandatory argument.
        Indicates the season you want UFL roster data from.

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
    A pandas `DataFrame` object with UFL roster data.

    """
    fox_key = get_fox_api_key()
    columns_order = [
        "season",
        "week",
        "team_id",
        "player_id",
        "player_analytics_name",
        "player_num",
        "player_name",
        "position",
        "player_age",
        "player_height",
        "player_weight",
        "college",
        "last_updated",
        "player_headshot",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36",
        # "Referer": "https://www.theufl.com/",
    }
    now = datetime.now(UTC).isoformat()
    temp_df = pd.DataFrame()
    roster_df = pd.DataFrame()
    roster_df_arr = []

    # Make the temp directories.
    try:
        mkdir("rosters/current_rosters")
    except Exception:
        logging.info("`rosters/current_rosters` already exists.")

    try:
        mkdir("rosters/weekly_rosters")
    except Exception:
        logging.info("`rosters/weekly_rosters` already exists.")

    # Get the current week for these rosters
    schedule_df = pd.read_parquet(
        "https://github.com/armstjc/ufl-data-repository/releases/download/"
        + f"ufl-standings/{season}_ufl_standings.parquet"
    )

    current_week = int(schedule_df["week"].max())
    # Doing this so the rosters are always up to date
    # with the current week.
    current_week += 1

    for t_id in tqdm(range(1, 9)):
        url = (
            "https://api.foxsports.com/bifrost/v1/ufl/team/"
            + f"{t_id}/roster?apikey={fox_key}"
        )

        response = requests.get(url, headers=headers)

        json_data = json.loads(response.text)

        for group in json_data["groups"]:
            # print(len(group["rows"]))
            if len(group["rows"]) > 1:
                for player in group["rows"]:
                    player_id = player["entityLink"]["layout"]["tokens"]["id"]
                    temp_df = pd.DataFrame({"player_id": player_id}, index=[0])

                    temp_df["player_analytics_name"] = player["entityLink"][
                        "analyticsName"
                    ]
                    temp_df["team_id"] = t_id
                    temp_df["player_num"] = player[
                        "columns"][0]["superscript"].replace("#", "")
                    temp_df["player_name"] = player["columns"][0]["text"]
                    temp_df["player_headshot"] = player[
                        "columns"][0]["imageUrl"]
                    temp_df["position"] = player["columns"][1]["text"]
                    temp_df["player_age"] = int(player["columns"][2]["text"])
                    temp_df["player_height_ft_in"] = player[
                        "columns"][3]["text"]
                    temp_df["player_weight"] = int(
                        player["columns"][4]["text"].replace(" lbs", "")
                    )
                    try:
                        temp_df["college"] = player["columns"][5]["text"]
                    except Exception:
                        temp_df["college"] = None

                    roster_df_arr.append(temp_df)
                    del temp_df, player_id

    roster_df = pd.concat(roster_df_arr, ignore_index=True)

    roster_df["season"] = season
    roster_df["week"] = current_week
    roster_df["last_updated"] = now

    roster_df[["height_ft", "height_in"]] = roster_df[
        "player_height_ft_in"].str.split("'", expand=True)
    roster_df["height_in"] = roster_df["height_in"].str.replace("\"", "")

    roster_df = roster_df.astype(
        {
            "player_id": "int64",
            "height_ft": "int16",
            "height_in": "int16",
        }
    )
    roster_df["player_height"] = roster_df["height_in"] + (
        roster_df["height_ft"] * 12
    )
    print(roster_df.dtypes)

    roster_df = roster_df[columns_order]
    roster_df.sort_values(
        by=[
            "season",
            "week",
            "team_id",
            "player_num",
        ]
    )

    if save_csv is True:
        roster_df.to_csv(
            f"rosters/current_rosters/{season}_ufl_rosters.csv", index=False
        )
        roster_df.to_csv(
            f"rosters/weekly_rosters/{season}-" +
            f"{current_week:02d}_ufl_rosters.csv",
            index=False,
        )

    if save_parquet is True:
        roster_df.to_parquet(
            f"rosters/current_rosters/{season}_ufl_rosters.parquet",
            index=False
        )
        roster_df.to_parquet(
            f"rosters/weekly_rosters/{season}-"
            + f"{current_week:02d}_ufl_rosters.parquet",
            index=False,
        )

    return roster_df


if __name__ == "__main__":
    now = datetime.now()

    parser = ArgumentParser()

    parser.add_argument(
        "--save_csv", default=False, action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_parquet", default=False, action=BooleanOptionalAction
    )

    args = parser.parse_args()

    ufl_roster_data(
        season=now.year, save_csv=args.save_csv, save_parquet=args.save_parquet
    )
