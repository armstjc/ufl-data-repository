"""
# Creation Date: 04/01/2024 03:00 PM EDT
# Last Updated Date: 04/03/2024 01:35 AM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: get_ufl_schedules.py
# Purpose: Allows one to get UFL schedule data.
###############################################################################
"""

from argparse import ArgumentParser, BooleanOptionalAction
from datetime import UTC, datetime
import json
import logging
from os import mkdir
# import time

import pandas as pd
import requests
from tqdm import tqdm

from utils import get_fox_api_key


def fox_sports_player_stats_parser(
    data: dict,
    team_id: int,
    team_abv: str,
    team_analytics_id: str,
    team_name: str,
    team_nickname: str,
) -> pd.DataFrame:
    """
    DO NOT CALL DIRECTLY!

    This is a helper function that parses player stats
    """
    stats_df = pd.DataFrame()
    stat_columns = [
        "team_id",
        "team_abv",
        "team_analytics_id",
        "team_name",
        "team_nickname",
        "player_id",
        "player_name",
        # Passing
        "passing_COMP",
        "passing_ATT",
        "passing_COMP%",
        "passing_YDS",
        "passing_TD",
        "passing_INT",
        "passing_Y/A",
        "passing_AY/A",
        "passing_Y/C",
        "passing_NFL_QBR",
        "passing_CFB_QBR",
        # Rushing
        "rushing_ATT",
        "rushing_YDS",
        "rushing_TD",
        "rushing_LONG",
        "rushing_AVG",
        # Receiving
        "receiving_TGT",
        "receiving_REC",
        "receiving_YDS",
        "receiving_AVG",
        "receiving_TD",
        "receiving_LONG",
        "receiving_CATCH%",
        "receiving_YDS/TGT",
        # Fumbles
        "fumbles_FUM",
        "fumbles_FUM_LOST",
        # Defense
        "defense_TAK",
        "defense_SOLO",
        "defense_AST",
        "defense_TFL",
        "defense_SACKS",
        "defense_INT",
        "defense_PD",
        "defense_TD",
        "defense_FF",
        "defense_FR",
        # Kicking (FG)
        "kicking_FGM",
        "kicking_FGA",
        "kicking_FG%",
        "kicking_FG_LONG",
        # Punting
        "punting_NO",
        "punting_AVG",
        "punting_IN_20",
        "punting_TB",
        "punting_LONG",
        "punting_BLK",
        # Kick Return
        "kick_return_KR",
        "kick_return_YDS",
        "kick_return_AVG",
        "kick_return_LONG",
        "kick_return_TD",
        # Punt Return
        "punt_return_PR",
        "punt_return_YDS",
        "punt_return_AVG",
        "punt_return_LONG",
        "punt_return_TD",
    ]

    passing_df = pd.DataFrame()
    passing_df_arr = []

    rushing_df = pd.DataFrame()
    rushing_df_arr = []

    receiving_df = pd.DataFrame()
    receiving_df_arr = []

    defense_df = pd.DataFrame()
    defense_df_arr = []

    fumbles_df = pd.DataFrame()
    fumbles_df_arr = []

    kick_return_df = pd.DataFrame()
    kick_return_df_arr = []

    punt_return_df = pd.DataFrame()
    punt_return_df_arr = []

    kicking_df = pd.DataFrame()
    kicking_df_arr = []

    punting_df = pd.DataFrame()
    punting_df_arr = []

    temp_df = pd.DataFrame()

    for stat in data:
        stat_type = stat["boxscoreTable"]["headers"][0]["columns"][0]["text"]

        if stat_type == "PASSING":
            # print("\n PASSING")
            for player in stat["boxscoreTable"]["rows"]:

                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["passing_COMP/ATT"] = player["columns"][1]["text"]
                # temp_df["passing_COMP%"] = player["columns"][2]["text"]
                temp_df["passing_YDS"] = player["columns"][3]["text"]
                # temp_df["passing_YDS/ATT"] = player["columns"][4]["text"]
                temp_df["passing_TD"] = player["columns"][5]["text"]
                temp_df["passing_INT"] = player["columns"][6]["text"]
                temp_df["passing_NFL_QBR"] = player["columns"][7]["text"]

                passing_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "RUSHING":
            # print("\n RUSHING")
            for player in stat["boxscoreTable"]["rows"]:

                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["rushing_ATT"] = player["columns"][1]["text"]
                temp_df["rushing_YDS"] = player["columns"][2]["text"]
                # temp_df["rushing_AVG"] = player["columns"][3]["text"]
                temp_df["rushing_TD"] = player["columns"][4]["text"]
                temp_df["rushing_LONG"] = player["columns"][5]["text"]

                rushing_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "RECEIVING":
            # print("\n RECEIVING")
            for player in stat["boxscoreTable"]["rows"]:

                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["receiving_REC"] = player["columns"][1]["text"]
                temp_df["receiving_YDS"] = player["columns"][2]["text"]
                # temp_df["receiving_AVG"] = player["columns"][3]["text"]
                temp_df["receiving_TD"] = player["columns"][4]["text"]
                temp_df["receiving_LONG"] = player["columns"][5]["text"]
                temp_df["receiving_TGT"] = player["columns"][6]["text"]

                receiving_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "DEFENSIVE":
            # print("\n DEFENSIVE")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )
                temp_df["defense_TAK"] = int(player["columns"][1]["text"])
                temp_df["defense_SOLO"] = int(player["columns"][2]["text"])
                temp_df["defense_SACKS"] = float(player["columns"][3]["text"])
                temp_df["defense_TFL"] = int(player["columns"][4]["text"])
                temp_df["defense_INT"] = int(player["columns"][5]["text"])
                temp_df["defense_PD"] = int(player["columns"][6]["text"])
                temp_df["defense_TD"] = int(player["columns"][7]["text"])

                defense_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "FUMBLES":
            # print("\n FUMBLES")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )
                temp_df["fumbles_FUM"] = int(player["columns"][1]["text"])
                temp_df["fumbles_FUM_LOST"] = int(
                    player["columns"][2]["text"]
                )
                temp_df["defense_FF"] = int(player["columns"][3]["text"])
                temp_df["defense_FR"] = int(player["columns"][4]["text"])

                fumbles_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "KICK RETURN":
            # print("\n KICK RETURN")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["kick_return_KR"] = int(player["columns"][1]["text"])
                temp_df["kick_return_YDS"] = int(player["columns"][2]["text"])
                # temp_df["kick_return_AVG"] = player["columns"][3]["text"]
                temp_df["kick_return_LONG"] = int(player["columns"][4]["text"])
                temp_df["kick_return_TD"] = int(player["columns"][5]["text"])

                kick_return_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "PUNT RETURN":
            # print("\n PUNT RETURN")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["punt_return_PR"] = int(player["columns"][1]["text"])
                temp_df["punt_return_YDS"] = int(player["columns"][2]["text"])
                # temp_df["punt_return_AVG"] = player["columns"][3]["text"]
                temp_df["punt_return_LONG"] = int(player["columns"][4]["text"])
                temp_df["punt_return_TD"] = int(player["columns"][5]["text"])

                punt_return_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "KICKING":
            # print("\n KICKING")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )
                temp_df["kicking_FG"] = player["columns"][1]["text"]
                temp_df["kicking_FG%"] = player["columns"][2]["text"]
                temp_df["kicking_FG_LONG"] = int(player["columns"][3]["text"])
                kicking_df_arr.append(temp_df)

                del temp_df

        elif stat_type == "PUNTING":
            # print("\n PUNTING")
            for player in stat["boxscoreTable"]["rows"]:
                if player["columns"][0]["text"] == "TOTALS":
                    break

                player_id = player["entityLink"]["layout"]["tokens"]["id"]

                try:
                    player_name = player["entityLink"]["imageAltText"]
                except Exception:
                    player_name = player["entityLink"]["title"]

                temp_df = pd.DataFrame(
                    {
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "team_analytics_id": team_analytics_id,
                        "team_name": team_name,
                        "team_nickname": team_nickname,
                        "player_id": player_id,
                        "player_name": player_name,
                    },
                    index=[0],
                )

                temp_df["punting_NO"] = int(player["columns"][1]["text"])
                temp_df["punting_AVG"] = float(player["columns"][2]["text"])
                temp_df["punting_IN_20"] = int(player["columns"][3]["text"])
                temp_df["punting_TB"] = int(player["columns"][4]["text"])
                temp_df["punting_LONG"] = int(player["columns"][5]["text"])
                temp_df["punting_BLK"] = int(player["columns"][6]["text"])

                punting_df_arr.append(temp_df)

                del temp_df

        else:
            raise ValueError(f"Unhandled stat type {stat_type}")

    # Passing
    if len(passing_df_arr) > 0:
        passing_df = pd.concat(passing_df_arr, ignore_index=True)

        passing_df[["passing_COMP", "passing_ATT"]] = passing_df[
            "passing_COMP/ATT"].str.split("/", expand=True)
        passing_df.drop(
            columns=["passing_COMP/ATT"],
            inplace=True
        )
        passing_df = passing_df.astype(
            {
                "passing_COMP": "uint16",
                "passing_ATT": "uint16",
                "passing_YDS": "uint16",
                "passing_TD": "uint16",
                "passing_INT": "uint16",
            }
        )
        passing_df.loc[passing_df["passing_ATT"] > 0, "passing_COMP%"] = round(
            passing_df["passing_COMP"] / passing_df["passing_ATT"],
            4
        )
        passing_df.loc[passing_df["passing_ATT"] > 0, "passing_Y/A"] = round(
            passing_df["passing_YDS"] / passing_df["passing_ATT"],
            3
        )
        passing_df.loc[passing_df["passing_ATT"] > 0, "passing_AY/A"] = round(
            (
                passing_df["passing_YDS"] +
                (passing_df["passing_TD"] * 20) +
                (passing_df["passing_INT"] * 45)
            ) / passing_df["passing_ATT"],
            3
        )
        passing_df.loc[passing_df["passing_COMP"] > 0, "passing_Y/C"] = round(
            passing_df["passing_YDS"] / passing_df["passing_COMP"],
            3
        )
        passing_df.loc[
            passing_df["passing_ATT"] > 0, "passing_CFB_QBR"
        ] = round(
            (
                (passing_df["passing_YDS"] * 8.4) +
                (passing_df["passing_TD"] * 330) +
                (passing_df["passing_COMP"] * 100) -
                (passing_df["passing_INT"] * 200)
            ) / passing_df["passing_ATT"],
            3
        )

    # Rushing
    if len(rushing_df_arr) > 0:
        rushing_df = pd.concat(rushing_df_arr, ignore_index=True)
        rushing_df = rushing_df.astype(
            {
                "rushing_ATT": "uint16",
                "rushing_YDS": "uint16",
                "rushing_TD": "uint16",
                "rushing_LONG": "uint16",
            }
        )

        rushing_df.loc[rushing_df["rushing_ATT"] > 0, "rushing_AVG"] = round(
            rushing_df["rushing_YDS"] / rushing_df["rushing_ATT"],
            3
        )

    # Receiving
    if len(receiving_df_arr) > 0:
        receiving_df = pd.concat(receiving_df_arr, ignore_index=True)
        receiving_df = receiving_df.astype(
            {
                "receiving_REC": "uint16",
                "receiving_YDS": "uint16",
                "receiving_TD": "uint16",
                "receiving_LONG": "uint16",
                "receiving_TGT": "uint16",
            }
        )
        receiving_df.loc[
            receiving_df["receiving_REC"] > 0, "receiving_AVG"
        ] = round(
            receiving_df["receiving_YDS"] / receiving_df["receiving_REC"],
            3
        )

        receiving_df.loc[
            receiving_df["receiving_TGT"] > 0, "receiving_CATCH%"
        ] = round(
            receiving_df["receiving_REC"] / receiving_df["receiving_TGT"],
            4
        )

        receiving_df.loc[
            receiving_df["receiving_TGT"] > 0, "receiving_YDS/TGT"
        ] = round(
            receiving_df["receiving_YDS"] / receiving_df["receiving_TGT"],
            3
        )

    # Defense
    if len(defense_df_arr) > 0:
        defense_df = pd.concat(defense_df_arr, ignore_index=True)
        defense_df["defense_AST"] = (
            defense_df["defense_TAK"] - defense_df["defense_SOLO"]
        )

    # Fumbles
    if len(fumbles_df_arr) > 0:
        fumbles_df = pd.concat(fumbles_df_arr, ignore_index=True)

    # Kick returns
    if len(kick_return_df_arr) > 0:
        kick_return_df = pd.concat(kick_return_df_arr, ignore_index=True)
        kick_return_df = kick_return_df.astype(
            {
                "kick_return_KR": "uint16",
                "kick_return_YDS": "uint16",
                "kick_return_LONG": "uint16",
                "kick_return_TD": "uint16",
            }
        )
        kick_return_df.loc[
            kick_return_df["kick_return_KR"] > 0, "kick_return_AVG"
        ] = (
            round(
                kick_return_df["kick_return_YDS"] /
                kick_return_df["kick_return_KR"],
                3
            )
        )

    # Punt Returns
    if len(punt_return_df_arr) > 0:
        punt_return_df = pd.concat(punt_return_df_arr, ignore_index=True)
        punt_return_df = punt_return_df.astype(
            {
                "punt_return_PR": "uint16",
                "punt_return_YDS": "uint16",
                "punt_return_LONG": "uint16",
                "punt_return_TD": "uint16",
            }
        )
        punt_return_df.loc[
            punt_return_df["punt_return_PR"] > 0, "punt_return_AVG"
        ] = (
            round(
                punt_return_df["punt_return_YDS"] /
                punt_return_df["punt_return_PR"],
                3
            )
        )

    # Kicking (FG)
    if len(kicking_df_arr) > 0:
        kicking_df = pd.concat(kicking_df_arr, ignore_index=True)
        kicking_df[["kicking_FGM", "kicking_FGA"]] = kicking_df[
            "kicking_FG"].str.split("/", expand=True)
        kicking_df.drop(
            columns=["kicking_FG"],
            inplace=True
        )
        kicking_df = kicking_df.astype(
            {
                "kicking_FGM": "uint16",
                "kicking_FGA": "uint16",
                "kicking_FG_LONG": "uint16"
            }
        )
        kicking_df.loc[kicking_df["kicking_FGA"] > 0, "kicking_FG%"] = round(
            kicking_df["kicking_FGM"] / kicking_df["kicking_FGA"],
            4
        )

    # Punting
    if len(punting_df_arr) > 0:
        punting_df = pd.concat(punting_df_arr, ignore_index=True)
        punting_df = punting_df.astype(
            {
                "punting_NO": "uint16",
                "punting_AVG": "float16",
                "punting_IN_20": "uint16",
                "punting_TB": "uint16",
                "punting_LONG": "uint16",
                "punting_BLK": "uint16",
            }
        )
        punting_df.loc[
            punting_df["punting_NO"] > 0, "punting_GROSS_YDS"
        ] = punting_df["punting_NO"] * punting_df["punting_AVG"]

    if len(passing_df) == 0 and rushing_df == 0:
        raise ValueError(
            "There isn't enough data here to make it worth " +
            "parsing this game."
        )

    if len(passing_df) > 0:
        stats_df = pd.merge(
            left=passing_df,
            right=rushing_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(receiving_df) > 0:
        stats_df = stats_df.merge(
            right=receiving_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(defense_df) > 0:
        stats_df = stats_df.merge(
            right=defense_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(fumbles_df) > 0:
        stats_df = stats_df.merge(
            right=fumbles_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(kick_return_df) > 0:
        stats_df = stats_df.merge(
            right=kick_return_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(punt_return_df) > 0:
        stats_df = stats_df.merge(
            right=punt_return_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(kicking_df) > 0:
        stats_df = stats_df.merge(
            right=kicking_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    if len(punting_df) > 0:
        stats_df = stats_df.merge(
            right=punting_df,
            how="outer",
            on=[
                "team_id",
                "team_abv",
                "team_analytics_id",
                "team_name",
                "team_nickname",
                "player_id",
                "player_name"
            ]
        )

    stats_df = stats_df.reindex(columns=stat_columns)
    # stats_df.to_csv('test.csv', index=False)
    return stats_df


def get_ufl_game_stats(
    season: int,
    parse_team_stats: bool = False,
    save_csv: bool = False,
    save_parquet: bool = False,
    save_json: bool = True,
):
    """
    Retrieves UFL game stats,
    and parses them into a pandas `DataFrame`.

    `season` (int, mandatory):
        Mandatory argument.
        Indicates the season you want UFL roster data from.

    `parse_team_stats` (bool, optional):
        Optional argument.
        If set to `True`, `get_ufl_standings()` will parse
        team game stats at the same time.

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
        the raw `.json` files for each UFL game.

    Returns
    ----------
    A pandas `DataFrame` object with UFL game stats.

    """
    fox_key = get_fox_api_key()
    columns_order = [
        "season",
        "league",
        "game_id",
        "team_id",
        "team_abv",
        "team_analytics_id",
        "team_name",
        "team_nickname",
        "score",
        "player_id",
        "player_name",
        "passing_COMP",
        "passing_ATT",
        "passing_COMP%",
        "passing_YDS",
        "passing_TD",
        "passing_INT",
        "passing_Y/A",
        "passing_AY/A",
        "passing_Y/C",
        "passing_NFL_QBR",
        "passing_CFB_QBR",
        "rushing_ATT",
        "rushing_YDS",
        "rushing_TD",
        "rushing_LONG",
        "rushing_AVG",
        "receiving_TGT",
        "receiving_REC",
        "receiving_YDS",
        "receiving_AVG",
        "receiving_TD",
        "receiving_LONG",
        "receiving_CATCH%",
        "receiving_YDS/TGT",
        "fumbles_FUM",
        "fumbles_FUM_LOST",
        "defense_TAK",
        "defense_SOLO",
        "defense_AST",
        "defense_TFL",
        "defense_SACKS",
        "defense_INT",
        "defense_PD",
        "defense_TD",
        "defense_FF",
        "defense_FR",
        "kicking_FGM",
        "kicking_FGA",
        "kicking_FG%",
        "kicking_FG_LONG",
        "punting_NO",
        "punting_AVG",
        "punting_IN_20",
        "punting_TB",
        "punting_LONG",
        "punting_BLK",
        "kick_return_KR",
        "kick_return_YDS",
        "kick_return_AVG",
        "kick_return_LONG",
        "kick_return_TD",
        "punt_return_PR",
        "punt_return_YDS",
        "punt_return_AVG",
        "punt_return_LONG",
        "punt_return_TD",
        "last_updated",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36",
        # "Referer": "https://www.theufl.com/",
    }
    now = datetime.now(UTC).isoformat()
    # temp_df = pd.DataFrame()
    stats_df = pd.DataFrame()
    stats_df_arr = []

    team_stats_df = pd.DataFrame()
    team_stats_df_arr = []

    schedule_df = pd.read_parquet(
        "https://github.com/armstjc/ufl-data-repository/releases/"
        + f"download/ufl-schedule/{season}_ufl_schedule.parquet"
    )

    schedule_df = schedule_df[
        (schedule_df["away_score"] > -1) | (schedule_df["home_score"] > -1)
    ]
    # print(schedule_df)

    ufl_game_id_arr = schedule_df["ufl_game_id"].to_numpy()

    for g_id in tqdm(ufl_game_id_arr):
        # Team stat declarations, because the way FOX Sports
        # stores this info is so cringe and bad,
        # we have to populate these variables
        # to make this code run fast.
        away_time_of_possession = None
        away_total_drives = None
        away_total_plays = None
        away_total_yards = None
        away_yards_per_play = None
        away_rz_td = None
        away_rz_att = None
        away_turnovers = None

        home_time_of_possession = None
        home_total_drives = None
        home_total_plays = None
        home_total_yards = None
        home_yards_per_play = None
        home_rz_td = None
        home_rz_att = None
        home_turnovers = None

        url = (
            "https://api.foxsports.com/bifrost/v1/ufl/event/"
            + f"{g_id}/data?apikey={fox_key}"
        )
        response = requests.get(url=url, headers=headers)
        game_id = int(g_id)
        game_json = json.loads(response.text)

        if save_json is True:
            with open(f"raw_game_json/ufl_game_{game_id:03d}.json", "w+") as f:
                f.write(json.dumps(game_json, indent=4))

        league_id = game_json["header"]["analyticsSport"]

        game_datetime = game_json["header"]["eventTime"]
        game_datetime = datetime.fromisoformat(game_datetime)

        away_team_id = int(
            game_json[
                "header"]["leftTeam"]["entityLink"]["layout"]["tokens"]["id"]
        )
        away_team_abv = game_json["header"]["leftTeam"]["name"]
        away_team_analytics_id = game_json["header"]["leftTeam"]["entityLink"][
            "analyticsName"
        ]
        away_team_name = game_json["header"]["leftTeam"]["alternateName"]
        away_team_nickname = str(
            game_json["header"]["leftTeam"]["longName"]
        ).upper()
        away_team_score = int(game_json["header"]["leftTeam"]["score"])
        away_team_loser_flag = game_json["header"]["leftTeam"]["isLoser"]

        home_team_id = int(
            game_json[
                "header"]["rightTeam"]["entityLink"]["layout"]["tokens"]["id"]
        )
        home_team_abv = game_json["header"]["rightTeam"]["name"]
        home_team_analytics_id = game_json[
            "header"]["rightTeam"]["entityLink"]["analyticsName"]
        home_team_name = game_json["header"]["rightTeam"]["alternateName"]
        home_team_nickname = str(
            game_json["header"]["rightTeam"]["longName"]
        ).upper()
        home_team_score = int(game_json["header"]["leftTeam"]["score"])
        home_team_loser_flag = game_json["header"]["leftTeam"]["isLoser"]

        for team in game_json["boxscore"]["boxscoreSections"]:

            if team["title"] == "MATCHUP" and parse_team_stats is True:

                for b in team["boxscoreMatchup"]:

                    if b["title"] == "POSSESSION" or b["title"] == "TURNOVERS":
                        # Yes, this is how nested team stats are.
                        for r in b["rows"]:
                            # "POSSESSION"
                            if r["title"] == "Time Of Possession":
                                away_time_of_possession = r["leftStat"]
                                home_time_of_possession = r["rightStat"]

                            elif r["title"] == "Total Drives":
                                away_total_drives = r["leftStat"]
                                home_total_drives = r["rightStat"]

                            elif r["title"] == "Total Plays":
                                away_total_plays = r["leftStat"]
                                home_total_plays = r["rightStat"]

                            elif r["title"] == "Total Yards":
                                away_total_yards = r["leftStat"]
                                home_total_yards = r["rightStat"]

                            elif r["title"] == "Yards Per Play":
                                away_yards_per_play = r["leftStat"]
                                home_yards_per_play = r["rightStat"]

                            elif r["title"] == "Red Zone TDs":
                                away_rz_td = r["leftStat"]
                                home_rz_td = r["rightStat"]

                            elif r["title"] == "Red Zone Attempts":
                                away_rz_att = r["leftStat"]
                                home_rz_att = r["rightStat"]

                            # "TURNOVERS"
                            elif r["title"] == "Total Drives" \
                                    and r["title"] == "Total":
                                away_turnovers = r["leftStat"]
                                home_turnovers = r["rightStat"]

                temp_df = pd.DataFrame(
                    {
                        "season": season,
                        "league": league_id,
                        "team_id": [home_team_id, away_team_id],
                        "game_id": [g_id, g_id],
                        "time_of_possession": [
                            home_time_of_possession,
                            away_time_of_possession
                        ],
                        "total_drives": [home_total_drives, away_total_drives],
                        "total_plays": [home_total_plays, away_total_plays],
                        "total_yards": [home_total_yards, away_total_yards],
                        "yards_per_play": [
                            home_yards_per_play,
                            away_yards_per_play
                        ],
                        "redzone_TDs": [home_rz_td, away_rz_td],
                        "redzone_attempts": [home_rz_att, away_rz_att],
                        "turnovers": [home_turnovers, away_turnovers]

                    },
                )
                team_stats_df_arr.append(temp_df)

                del temp_df

            elif team["title"] == "MATCHUP" and parse_team_stats is False:
                pass

            elif team["title"] == away_team_nickname:
                # print(away_team_nickname)
                temp_df = fox_sports_player_stats_parser(
                    team["boxscoreItems"],
                    away_team_id,
                    away_team_abv,
                    away_team_analytics_id,
                    away_team_name,
                    away_team_nickname,
                )

                if away_team_loser_flag is False:
                    temp_df["score"] = f"W {away_team_score}-{home_team_score}"
                else:
                    temp_df["score"] = f"L {away_team_score}-{home_team_score}"
                temp_df["game_id"] = g_id
                stats_df_arr.append(temp_df)

                del temp_df

            elif team["title"] == home_team_nickname:
                temp_df = fox_sports_player_stats_parser(
                    team["boxscoreItems"],
                    home_team_id,
                    home_team_abv,
                    home_team_analytics_id,
                    home_team_name,
                    home_team_nickname,
                )

                if home_team_loser_flag is False:
                    temp_df["score"] = f"W {home_team_score}-{away_team_score}"
                else:
                    temp_df["score"] = f"L {home_team_score}-{away_team_score}"
                temp_df["game_id"] = g_id
                stats_df_arr.append(temp_df)

                del temp_df

            else:
                bad_title = team["title"]
                raise ValueError(f"Unhandled boxscore type {bad_title}")

    del fox_key

    stats_df = pd.concat(stats_df_arr, ignore_index=True)
    stats_df["season"] = season
    stats_df["league"] = league_id
    # stats_df["game_id"] =
    stats_df["last_updated"] = now

    if parse_team_stats is True:
        team_stats_df = pd.concat(
            team_stats_df_arr,
            ignore_index=True
        )
        # print(team_stats_df)
        team_stats_sum = stats_df.groupby(
            ["season", "league", "team_id", "game_id"], as_index=False
        )[[
            "passing_COMP",
            "passing_ATT",
            "passing_YDS",
            "passing_TD",
            "passing_INT",
            "rushing_ATT",
            "rushing_YDS",
            "rushing_TD",
            # "receiving_TGT",
            # "receiving_REC",
            # "receiving_YDS",
            # "receiving_TD",
            "fumbles_FUM",
            "fumbles_FUM_LOST",
            "defense_TAK",
            "defense_SOLO",
            "defense_AST",
            "defense_TFL",
            "defense_SACKS",
            "defense_INT",
            "defense_PD",
            "defense_TD",
            "defense_FF",
            "defense_FR",
            "kicking_FGM",
            "kicking_FGA",
            "punting_NO",
            "punting_AVG",
            "punting_IN_20",
            "punting_TB",
            "punting_BLK",
            "kick_return_KR",
            "kick_return_YDS",
            # "kick_return_AVG",
            "kick_return_TD",
            "punt_return_PR",
            "punt_return_YDS",
            # "punt_return_AVG",
            "punt_return_TD",
        ]].sum()

        team_stats_max = stats_df.groupby(
            ["season", "league", "team_id", "game_id"],
        )[[
            "kicking_FG_LONG",
            "rushing_LONG",
            # "receiving_LONG",
            "punting_LONG",
            "kick_return_LONG",
            "punt_return_LONG",
        ]].max()

        team_stats_df = team_stats_df.merge(
            team_stats_sum,
            how="outer",
            on=["season", "league", "team_id", "game_id"]
        )

        team_stats_df = team_stats_df.merge(
            team_stats_max,
            how="outer",
            on=["season", "league", "team_id", "game_id"]
        )

        # print(team_stats_df)

    # print(stats_df.columns)
    stats_df = stats_df[columns_order]

    if save_csv is True:
        stats_df.to_csv(
            f"game_stats/player/{season}_ufl_player_game_stats.csv",
            index=False
        )

        if len(team_stats_df) > 0:
            stats_df.to_csv(
                f"game_stats/team/{season}_ufl_team_game_stats.csv",
                index=False
            )
    if save_parquet is True:
        stats_df.to_parquet(
            f"game_stats/player/{season}_ufl_player_game_stats.parquet",
            index=False
        )
        if len(team_stats_df) > 0:
            stats_df.to_parquet(
                f"game_stats/team/{season}_ufl_team_game_stats.parquet",
                index=False
            )


if __name__ == "__main__":
    now = datetime.now()

    try:
        mkdir("game_stats/player")
    except Exception as e:
        logging.warning(
            f"Unhandled exception {e}"
        )

    try:
        mkdir("game_stats/team")
    except Exception as e:
        logging.warning(
            f"Unhandled exception {e}"
        )
    parser = ArgumentParser()

    parser.add_argument(
        "--save_csv", default=False, action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_parquet", default=False, action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_json", default=False, action=BooleanOptionalAction
    )

    args = parser.parse_args()

    get_ufl_game_stats(
        season=now.year,
        parse_team_stats=True,
        save_csv=args.save_csv,
        save_parquet=args.save_parquet,
        save_json=args.save_json,
    )
