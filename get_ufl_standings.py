"""
# Creation Date: 03/29/2024 06:27 PM EDT
# Last Updated Date: 03/30/2024 02:05 PM EDT
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

import numpy as np
import pandas as pd
import requests
from utils import get_fox_api_key

# from bs4 import BeautifulSoup


def get_ufl_standings(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False,
    save_json: bool = False,
):
    """
    Retrieves the current standings from the UFL,
    parses the data.

    Parameters
    ----------

    `season` (int, mandatory):
        Mandatory argument.
        Indicates the season you want UFL standings data from.

    `parse_league_standings` (bool, optional):
        Optional argument.
        If set to `True`,
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
    fox_key = get_fox_api_key()

    # Defaults
    columns_order = [
        "season",
        "week",
        "team_id",
        "team_analytics_id",
        "team_name",
        "rank",
        "G",
        "W",
        "L",
        "W%",
        "PF",
        "PA",
        "home_W",
        "home_L",
        "away_W",
        "away_L",
        "div_W",
        "div_L",
        "streak",
        "team_logo",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36"
    }
    url = (
        "https://api.foxsports.com/bifrost/v1/ufl/league/standings"
        + f"?season={season}&apikey={fox_key}"
    )

    del fox_key

    now = datetime.now(UTC).isoformat()
    temp_df = pd.DataFrame()
    conf_standings_df = pd.DataFrame()
    lg_standings_df = pd.DataFrame()

    conf_standings_df_arr = []
    lg_standings_df_arr = []

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

    # orj_json_text = response.text
    # json_text = orj_json_text[:-1].replace("callback({", "{")

    # del orj_json_text

    json_data = json.loads(response.text)
    conf_json_data = {}
    lg_json_data = {}

    for i in json_data["standingsSections"]:
        if i["title"] == "CONFERENCE":
            conf_json_data = i
            conf_json_data = conf_json_data["standings"]
        elif i["title"] == "LEAGUE":
            lg_json_data = i
            lg_json_data = lg_json_data["standings"][0]

    if save_json is False:
        del json_data

    # Conference Standings
    for s in conf_json_data:

        for team in s["rows"]:
            team_analytics_id = team["entityLink"]["analyticsName"]
            team_name = team["entityLink"]["title"].title()
            team_id = int(team["entityLink"]["layout"]["tokens"]["id"])
            team_logo = team["entityLink"]["imageUrl"]
            # print(team)
            temp_df = pd.DataFrame(
                {
                    "team_id": team_id,
                    "team_analytics_id": team_analytics_id,
                    "team_name": team_name,
                },
                index=[0],
            )
            temp_df["rank"] = team["columns"][0]["text"]
            temp_df["W-L"] = team["columns"][2]["text"]
            temp_df["W%"] = team["columns"][3]["text"]
            temp_df["PF"] = team["columns"][4]["text"]
            temp_df["PA"] = team["columns"][5]["text"]
            temp_df["home_W-L"] = team["columns"][6]["text"]
            temp_df["away_W-L"] = team["columns"][7]["text"]
            temp_df["div_W-L"] = team["columns"][8]["text"]
            temp_df["streak"] = team["columns"][9]["text"]
            temp_df["team_logo"] = team_logo

            conf_standings_df_arr.append(temp_df)

            del temp_df
    conf_standings_df = pd.concat(conf_standings_df_arr, ignore_index=True)
    del conf_standings_df_arr

    conf_standings_df[["W", "L"]] = conf_standings_df["W-L"].str.split("-", expand=True)

    conf_standings_df[["home_W", "home_L"]] = conf_standings_df["home_W-L"].str.split(
        "-", expand=True
    )

    conf_standings_df[["away_W", "away_L"]] = conf_standings_df["away_W-L"].str.split(
        "-", expand=True
    )

    conf_standings_df[["div_W", "div_L"]] = conf_standings_df["div_W-L"].str.split(
        "-", expand=True
    )
    conf_standings_df.replace("-", np.nan, inplace=True).replace(
        "", np.nan, inplace=True
    )
    conf_standings_df = conf_standings_df.fillna(0)

    conf_standings_df = conf_standings_df.astype(
        {
            "W": "int64",
            "L": "int64",
            "home_W": "int64",
            "home_L": "int64",
            "away_W": "int64",
            "away_L": "int64",
            "div_W": "int64",
            "div_L": "int64",
        }
    )

    conf_standings_df["G"] = conf_standings_df["W"] + conf_standings_df["L"]
    max_games = int(conf_standings_df["G"].max())
    conf_standings_df["season"] = season
    conf_standings_df["week"] = max_games

    # League Standings
    for team in lg_json_data["rows"]:
        team_analytics_id = team["entityLink"]["analyticsName"]
        team_name = team["entityLink"]["title"].title()
        team_id = int(team["entityLink"]["layout"]["tokens"]["id"])
        team_logo = team["entityLink"]["imageUrl"]
        # print(team)
        temp_df = pd.DataFrame(
            {
                "team_id": team_id,
                "team_analytics_id": team_analytics_id,
                "team_name": team_name,
            },
            index=[0],
        )
        temp_df["rank"] = team["columns"][0]["text"]
        temp_df["W-L"] = team["columns"][2]["text"]
        temp_df["W%"] = team["columns"][3]["text"]
        temp_df["PF"] = team["columns"][4]["text"]
        temp_df["PA"] = team["columns"][5]["text"]
        temp_df["home_W-L"] = team["columns"][6]["text"]
        temp_df["away_W-L"] = team["columns"][7]["text"]
        temp_df["div_W-L"] = team["columns"][8]["text"]
        temp_df["streak"] = team["columns"][9]["text"]
        temp_df["team_logo"] = team_logo

        lg_standings_df_arr.append(temp_df)

        del temp_df
    lg_standings_df = pd.concat(lg_standings_df_arr, ignore_index=True)
    del lg_standings_df_arr

    lg_standings_df[["W", "L"]] = lg_standings_df["W-L"].str.split("-", expand=True)

    lg_standings_df[["home_W", "home_L"]] = lg_standings_df["home_W-L"].str.split(
        "-", expand=True
    )

    lg_standings_df[["away_W", "away_L"]] = lg_standings_df["away_W-L"].str.split(
        "-", expand=True
    )

    lg_standings_df[["div_W", "div_L"]] = lg_standings_df["div_W-L"].str.split(
        "-", expand=True
    )
    lg_standings_df.replace(
        "-", np.nan, inplace=True).replace(
            "", np.nan, inplace=True)
    lg_standings_df = lg_standings_df.fillna(0)

    lg_standings_df = lg_standings_df.astype(
        {
            "W": "int64",
            "L": "int64",
            "home_W": "int64",
            "home_L": "int64",
            "away_W": "int64",
            "away_L": "int64",
            "div_W": "int64",
            "div_L": "int64",
        }
    )

    lg_standings_df["G"] = lg_standings_df["W"] + lg_standings_df["L"]
    max_games = int(lg_standings_df["G"].max())
    lg_standings_df["season"] = season
    lg_standings_df["week"] = max_games

    # schedule_df = schedule_df[columns_order]

    if max_games == 0:
        max_games += 1

    conf_standings_df = conf_standings_df[columns_order]
    lg_standings_df = lg_standings_df[columns_order]
    print(conf_standings_df.dtypes)
    print(lg_standings_df.dtypes)

    if save_csv is True:
        conf_standings_df.to_csv(
            "standings/current_standings/" + f"{season}_ufl_conference_standings.csv",
            index=False,
        )
        conf_standings_df.to_csv(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_conference_standings.csv",
            index=False,
        )
        lg_standings_df.to_csv(
            f"standings/current_standings/{season}_ufl_league_standings.csv",
            index=False,
        )
        lg_standings_df.to_csv(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_league_standings.csv",
            index=False,
        )

    if save_parquet is True:
        conf_standings_df.to_parquet(
            "standings/current_standings/"
            + f"{season}_ufl_conference_standings.parquet",
            index=False,
        )
        conf_standings_df.to_parquet(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_conference_standings.parquet",
            index=False,
        )
        lg_standings_df.to_csv(
            "standings/current_standings/" + f"{season}_ufl_league_standings.parquet",
            index=False,
        )
        lg_standings_df.to_csv(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_league_standings.parquet",
            index=False,
        )

    if save_json is True:

        json_data["access_time"] = now

        json_data["season"] = season
        json_data["week"] = max_games
        with open(
            f"standings/current_standings/{season}_ufl_standings.json", "w+"
        ) as f:
            f.write(
                json.dumps(
                    json_data,
                    indent=4,
                )
            )

        with open(
            f"standings/weekly_standings/{season}"
            + f"-{max_games:02d}_ufl_standings.json",
            "w+",
        ) as f:
            f.write(
                json.dumps(
                    json_data,
                    indent=4,
                )
            )


if __name__ == "__main__":
    now = datetime.now()

    parser = ArgumentParser()

    parser.add_argument("--save_csv", default=False, action=BooleanOptionalAction)
    parser.add_argument("--save_parquet", default=False, action=BooleanOptionalAction)
    parser.add_argument("--save_json", default=False, action=BooleanOptionalAction)

    args = parser.parse_args()

    get_ufl_standings(
        season=now.year,
        save_csv=args.save_csv,
        save_parquet=args.save_parquet,
        save_json=args.save_json,
    )
