"""
# Creation Date: 03/30/2024 03:41 PM EDT
# Last Updated Date: 04/01/2024 12:56 AM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: get_ufl_schedules.py
# Purpose: Allows one to get UFL schedule data.
###############################################################################
"""

import json
from argparse import ArgumentParser, BooleanOptionalAction
from datetime import UTC, datetime
import logging
from os import mkdir

# import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from utils import get_fox_api_key

# from bs4 import BeautifulSoup


def get_ufl_schedules(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False,
):
    """
    Retrieves schedule data from the UFL,
    and parses the data.

    Parameters
    ----------

    `season` (int, mandatory):
        Mandatory argument.
        Indicates the season you want UFL schedule data from.

    `save_csv` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will save
        the resulting `DataFrame` to a `.csv` file.

    `save_parquet` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will save
        the resulting `DataFrame` to a `.parquet` file.

    Returns
    ----------
    A pandas `DataFrame` object with UFL schedule data.

    """
    fox_key = get_fox_api_key()

    # Defaults
    columns_order = [
        "season",
        "season_type",
        "week_id",
        "week_title",
        "ufl_game_id",
        "game_date",
        "away_team_id",
        "away_team_analytics_name",
        "away_team_name",
        "home_team_id",
        "home_team_analytics_name",
        "home_team_name",
        "away_score",
        "home_score",
        "stadium",
        "location",
        "fox_bet_odds",
        "scheduled_date",
        "broadcast_network",
        "last_updated"
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36"
    }
    url = (
        "https://api.foxsports.com/bifrost/v1/ufl/league/schedule"
        + f"?season={season}&apikey={fox_key}"
    )

    # del fox_key

    now = datetime.now(UTC).isoformat()
    temp_df = pd.DataFrame()
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

    season_week_json = json.loads(response.text)

    for s_type in season_week_json["selectionGroupList"]:
        season_type = s_type["title"]
        print(f"Getting all {season_type.title()} games in {season}.")

        for week in tqdm(s_type["selectionList"]):
            week_id = week["id"]
            week_title = week["title"]

            week_url = week["uri"]
            week_url = f"{week_url}?apikey={fox_key}"
            response = requests.get(week_url, headers=headers)

            json_data = json.loads(response.text)
            for day in json_data["tables"]:
                u_date = day["title"]
                u_date = f"{u_date} {season}"
                game_date = datetime.strptime(u_date, "%a, %b %d %Y")

                for game in day["rows"]:
                    temp_df = pd.DataFrame(
                        {
                            "week_id": week_id,
                            "season_type": season_type,
                            "season": season,
                            "week_title": week_title,
                            "game_date": game_date,
                        },
                        index=[0],
                    )
                    try:
                        temp_df["away_team_id"] = game[
                            "linkList"
                        ][0]["entityLink"]["layout"]["tokens"]["id"]
                        temp_df["away_team_analytics_name"] = game[
                            "linkList"][0]["entityLink"]["analyticsName"]
                        temp_df["away_team_name"] = game[
                            "linkList"][0]["entityLink"]["title"].title()
                    except Exception:
                        temp_df["away_team_id"] = None
                        temp_df["away_team_analytics_name"] = None
                        temp_df["away_team_name"] = "TBD"

                    try:
                        temp_df["home_team_id"] = game[
                            "linkList"
                        ][1]["entityLink"]["layout"]["tokens"]["id"]
                        temp_df["home_team_analytics_name"] = game[
                            "linkList"][1]["entityLink"]["analyticsName"]
                        temp_df["home_team_name"] = game[
                            "linkList"][1]["entityLink"]["title"].title()
                    except Exception:
                        temp_df["home_team_id"] = None
                        temp_df["home_team_analytics_name"] = None
                        temp_df["home_team_name"] = "TBD"

                    if game["columns"][3]["subtext"] != "FINAL":
                        temp_df["scheduled_date"] = game["columns"][3]["text"]
                        temp_df["broadcast_network"] = game[
                            "columns"][3]["subtext"]
                    else:
                        score = game["columns"][3]["text"]
                        away_score, home_score = score.split("-")
                        temp_df["away_score"] = int(away_score)
                        temp_df["home_score"] = int(home_score)

                    temp_df["stadium"] = game["columns"][4]["text"]
                    temp_df["location"] = game["columns"][4]["subtext"]

                    try:
                        temp_df["fox_bet_odds"] = game["columns"][5]["text"]
                    except Exception:
                        temp_df["fox_bet_odds"] = None

                    temp_df["ufl_game_id"] = game[
                        "linkList"][2]["entityLink"]["layout"]["tokens"]["id"]

                    schedule_df_arr.append(temp_df)
                    del temp_df

    schedule_df = pd.concat(schedule_df_arr, ignore_index=True)
    schedule_df["last_updated"] = now
    schedule_df = schedule_df[columns_order]

    if save_csv is True:
        schedule_df.to_csv(
            "schedule/" + f"{season}_ufl_schedule.csv",
            index=False,
        )

    if save_parquet is True:
        schedule_df.to_parquet(
            "schedule/" + f"{season}_ufl_schedule.parquet",
            index=False,
        )


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

    get_ufl_schedules(
        season=now.year, save_csv=args.save_csv, save_parquet=args.save_parquet
    )
