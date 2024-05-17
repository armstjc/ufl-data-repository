"""
# Creation Date: 05/17/2024 01:20 PM EDT
# Last Updated Date: 05/17/2024 06:45 PM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: parse_ufl_season_stats.py
# Purpose: Allows one to get UFL season stats.
###############################################################################
"""

from datetime import UTC, datetime
import logging
from os import mkdir
import pandas as pd

# from utils import format_folder_path


def parse_ufl_player_season_stats(season: int):
    """ """
    columns = [
        "season",
        "league",
        "team_id",
        "team_abv",
        "team_analytics_id",
        "team_name",
        "team_nickname",
        "player_id",
        "player_name",
        "games_played",
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
        "punting_GROSS_YDS",
        "punting_GROSS_AVG",
        "punting_IN_20",
        "punting_IN_20%",
        "punting_TB",
        "punting_TB%",
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

    base_df = pd.read_csv(
        f"game_stats/player/{season}_ufl_player_game_stats.csv"
    )

    final_df = base_df.groupby(
        [
            "season",
            "league",
            "team_id",
            "team_abv",
            "team_analytics_id",
            "team_name",
            "team_nickname",
            "player_id",
            "player_name",
        ],
        group_keys=False,
        as_index=False,
    ).agg(
        {
            "game_id": "count",
            "passing_COMP": "sum",
            "passing_ATT": "sum",
            "passing_YDS": "sum",
            "passing_TD": "sum",
            "passing_INT": "sum",
            "rushing_ATT": "sum",
            "rushing_YDS": "sum",
            "rushing_TD": "sum",
            "rushing_LONG": "max",
            "receiving_TGT": "sum",
            "receiving_REC": "sum",
            "receiving_YDS": "sum",
            "receiving_TD": "sum",
            "receiving_LONG": "max",
            "fumbles_FUM": "sum",
            "fumbles_FUM_LOST": "sum",
            "defense_TAK": "sum",
            "defense_SOLO": "sum",
            "defense_AST": "sum",
            "defense_TFL": "sum",
            "defense_SACKS": "sum",
            "defense_INT": "sum",
            "defense_PD": "sum",
            "defense_TD": "sum",
            "defense_FF": "sum",
            "defense_FR": "sum",
            "kicking_FGM": "sum",
            "kicking_FGA": "sum",
            "kicking_FG_LONG": "max",
            "punting_NO": "sum",
            "punting_GROSS_YDS": "sum",
            "punting_IN_20": "sum",
            "punting_TB": "sum",
            "punting_LONG": "max",
            "punting_BLK": "sum",
            "kick_return_KR": "sum",
            "kick_return_YDS": "sum",
            "kick_return_LONG": "max",
            "kick_return_TD": "sum",
            "punt_return_PR": "sum",
            "punt_return_YDS": "sum",
            "punt_return_LONG": "max",
            "punt_return_TD": "sum",
        }
    )

    final_df.rename(columns={"game_id": "games_played"}, inplace=True)
    final_df.loc[final_df["passing_ATT"] > 0, "passing_COMP%"] = (
        final_df["passing_COMP"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_Y/A"] = (
        final_df["passing_YDS"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_AY/A"] = (
        final_df["passing_YDS"]
        + (final_df["passing_TD"] * 20)
        - (final_df["passing_INT"] * 45)
    ) / final_df["passing_ATT"]
    final_df.loc[final_df["passing_COMP"] > 0, "passing_Y/C"] = (
        final_df["passing_YDS"] / final_df["passing_COMP"]
    )

    # NFL Passer Rating segments
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_A"] = (
        (final_df["passing_COMP"] / final_df["passing_ATT"]) - 0.3
    ) * 5
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_B"] = (
        (final_df["passing_YDS"] / final_df["passing_ATT"]) - 3
    ) * 0.25
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_C"] = (
        final_df["passing_TD"] / final_df["passing_ATT"]
    ) * 20
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_D"] = 2.375 - (
        (final_df["passing_INT"] / final_df["passing_ATT"]) * 25
    )

    # Yes, this is a required step in the formula.
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_A"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_B"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_C"] > 2.375, "passing_NFL_QBR_C"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_D"] > 2.375, "passing_NFL_QBR_D"
    ] = 2.375

    # See above comment.
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_A"] = 0
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_B"] = 0
    final_df.loc[final_df["passing_NFL_QBR_C"] < 0, "passing_NFL_QBR_C"] = 0
    final_df.loc[final_df["passing_NFL_QBR_D"] < 0, "passing_NFL_QBR_D"] = 0

    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR"] = (
        (
            final_df["passing_NFL_QBR_A"]
            + final_df["passing_NFL_QBR_B"]
            + final_df["passing_NFL_QBR_C"]
            + final_df["passing_NFL_QBR_D"]
        )
        / 6
    ) * 100

    final_df.loc[final_df["passing_ATT"] > 0, "passing_CFB_QBR"] = (
        (final_df["passing_YDS"] * 8.4)
        + (final_df["passing_COMP"] * 100)
        + (final_df["passing_TD"] * 330)
        - (final_df["passing_INT"] * 200)
    ) / final_df["passing_ATT"]

    final_df.loc[final_df["rushing_ATT"] > 0, "rushing_AVG"] = (
        final_df["rushing_YDS"] / final_df["rushing_ATT"]
    )

    final_df.loc[final_df["receiving_REC"] > 0, "receiving_AVG"] = (
        final_df["receiving_YDS"] / final_df["receiving_REC"]
    )

    final_df.loc[final_df["receiving_TGT"] > 0, "receiving_CATCH%"] = (
        final_df["receiving_REC"] / final_df["receiving_TGT"]
    )

    final_df.loc[final_df["receiving_TGT"] > 0, "receiving_YDS/TGT"] = (
        final_df["receiving_YDS"] / final_df["receiving_TGT"]
    )

    final_df.loc[final_df["kicking_FGA"] > 0, "kicking_FG%"] = (
        final_df["kicking_FGM"] / final_df["kicking_FGA"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_GROSS_AVG"] = (
        final_df["punting_GROSS_YDS"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_TB%"] = (
        final_df["punting_TB"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_IN_20%"] = (
        final_df["punting_IN_20"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["kick_return_KR"] > 0, "kick_return_AVG"] = (
        final_df["kick_return_YDS"] / final_df["kick_return_KR"]
    )

    final_df.loc[final_df["punt_return_PR"] > 0, "punt_return_AVG"] = (
        final_df["punt_return_YDS"] / final_df["punt_return_PR"]
    )
    final_df = final_df.reindex(columns=columns)

    final_df = final_df.round(
        {
            "passing_COMP%": 4,
            "passing_Y/A": 3,
            "passing_AY/A": 3,
            "passing_Y/C": 3,
            "passing_NFL_QBR": 3,
            "passing_CFB_QBR": 3,
            "rushing_AVG": 3,
            "receiving_AVG": 3,
            "receiving_CATCH%": 4,
            "receiving_YDS/TGT": 3,
            "kicking_FG%": 4,
            "punting_GROSS_AVG": 3,
            "punting_IN_20%": 4,
            "punting_TB%": 4,
            "kick_return_AVG": 3,
            "punt_return_AVG": 3,
        }
    )
    final_df["last_updated"] = datetime.now(UTC).isoformat()
    print(final_df)
    final_df.to_csv(
        f"season_stats/player/{season}_ufl_player_season_stats.csv",
        index=False
    )
    final_df.to_parquet(
        f"season_stats/player/{season}_ufl_player_season_stats.parquet",
        index=False
    )
    # print(base_df.columns)


def parse_ufl_team_season_stats(season: int):
    """ """
    columns = [
        "season",
        "league",
        "team_id",
        "games_played",
        "total_drives",
        "total_plays",
        "total_yards",
        "yards_per_play",
        "redzone_TDs",
        "redzone_attempts",
        "turnovers",
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
        "punting_GROSS_YDS",
        "punting_GROSS_AVG",
        "punting_IN_20",
        "punting_IN_20%",
        "punting_TB",
        "punting_TB%",
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
    base_df = pd.read_csv(f"game_stats/team/{season}_ufl_team_game_stats.csv")

    # base_df["time_of_possession"] = pd.to_datetime(
    #     base_df["time_of_possession"],
    #     format="%M:%S"
    # ).dt.time

    # base_df["TOP"] = base_df["time_of_possession"].dt.total_seconds()
    final_df = base_df.groupby(
        [
            "season",
            "league",
            "team_id",
        ],
        group_keys=False,
        as_index=False,
    ).agg(
        {
            # "TOP": "sum",
            "game_id": "count",
            "total_drives": "sum",
            "total_plays": "sum",
            "total_yards": "sum",
            # "yards_per_play",
            "redzone_TDs": "sum",
            "redzone_attempts": "sum",
            "turnovers": "sum",
            "passing_COMP": "sum",
            "passing_ATT": "sum",
            "passing_YDS": "sum",
            "passing_TD": "sum",
            "passing_INT": "sum",
            "rushing_ATT": "sum",
            "rushing_YDS": "sum",
            "rushing_TD": "sum",
            "rushing_LONG": "max",
            "fumbles_FUM": "sum",
            "fumbles_FUM_LOST": "sum",
            "defense_TAK": "sum",
            "defense_SOLO": "sum",
            "defense_AST": "sum",
            "defense_TFL": "sum",
            "defense_SACKS": "sum",
            "defense_INT": "sum",
            "defense_PD": "sum",
            "defense_TD": "sum",
            "defense_FF": "sum",
            "defense_FR": "sum",
            "kicking_FGM": "sum",
            "kicking_FGA": "sum",
            "kicking_FG_LONG": "max",
            "punting_NO": "sum",
            "punting_GROSS_YDS": "sum",
            # "punting_AVG",
            "punting_IN_20": "sum",
            "punting_TB": "sum",
            "punting_BLK": "sum",
            "punting_LONG": "max",
            "kick_return_KR": "sum",
            "kick_return_YDS": "sum",
            "kick_return_TD": "sum",
            "kick_return_LONG": "max",
            "punt_return_PR": "sum",
            "punt_return_YDS": "sum",
            "punt_return_TD": "sum",
            "punt_return_LONG": "max",
        }
    )
    final_df.rename(columns={"game_id": "games_played"}, inplace=True)

    final_df.loc[final_df["total_plays"] > 0, "yards_per_play"] = (
        final_df["total_yards"] / final_df["total_plays"]
    )

    final_df.loc[final_df["passing_ATT"] > 0, "passing_COMP%"] = (
        final_df["passing_COMP"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_Y/A"] = (
        final_df["passing_YDS"] / final_df["passing_ATT"]
    )
    final_df.loc[final_df["passing_ATT"] > 0, "passing_AY/A"] = (
        final_df["passing_YDS"]
        + (final_df["passing_TD"] * 20)
        - (final_df["passing_INT"] * 45)
    ) / final_df["passing_ATT"]
    final_df.loc[final_df["passing_COMP"] > 0, "passing_Y/C"] = (
        final_df["passing_YDS"] / final_df["passing_COMP"]
    )

    # NFL Passer Rating segments
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_A"] = (
        (final_df["passing_COMP"] / final_df["passing_ATT"]) - 0.3
    ) * 5
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_B"] = (
        (final_df["passing_YDS"] / final_df["passing_ATT"]) - 3
    ) * 0.25
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_C"] = (
        final_df["passing_TD"] / final_df["passing_ATT"]
    ) * 20
    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR_D"] = 2.375 - (
        (final_df["passing_INT"] / final_df["passing_ATT"]) * 25
    )

    # Yes, this is a required step in the formula.
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_A"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_A"] > 2.375, "passing_NFL_QBR_B"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_C"] > 2.375, "passing_NFL_QBR_C"
    ] = 2.375
    final_df.loc[
        final_df["passing_NFL_QBR_D"] > 2.375, "passing_NFL_QBR_D"
    ] = 2.375

    # See above comment.
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_A"] = 0
    final_df.loc[final_df["passing_NFL_QBR_A"] < 0, "passing_NFL_QBR_B"] = 0
    final_df.loc[final_df["passing_NFL_QBR_C"] < 0, "passing_NFL_QBR_C"] = 0
    final_df.loc[final_df["passing_NFL_QBR_D"] < 0, "passing_NFL_QBR_D"] = 0

    final_df.loc[final_df["passing_ATT"] > 0, "passing_NFL_QBR"] = (
        (
            final_df["passing_NFL_QBR_A"]
            + final_df["passing_NFL_QBR_B"]
            + final_df["passing_NFL_QBR_C"]
            + final_df["passing_NFL_QBR_D"]
        )
        / 6
    ) * 100

    final_df.loc[final_df["passing_ATT"] > 0, "passing_CFB_QBR"] = (
        (final_df["passing_YDS"] * 8.4)
        + (final_df["passing_COMP"] * 100)
        + (final_df["passing_TD"] * 330)
        - (final_df["passing_INT"] * 200)
    ) / final_df["passing_ATT"]

    final_df.loc[final_df["rushing_ATT"] > 0, "rushing_AVG"] = (
        final_df["rushing_YDS"] / final_df["rushing_ATT"]
    )

    final_df.loc[final_df["kicking_FGA"] > 0, "kicking_FG%"] = (
        final_df["kicking_FGM"] / final_df["kicking_FGA"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_GROSS_AVG"] = (
        final_df["punting_GROSS_YDS"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_TB%"] = (
        final_df["punting_TB"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["punting_NO"] > 0, "punting_IN_20%"] = (
        final_df["punting_IN_20"] / final_df["punting_NO"]
    )

    final_df.loc[final_df["kick_return_KR"] > 0, "kick_return_AVG"] = (
        final_df["kick_return_YDS"] / final_df["kick_return_KR"]
    )

    final_df.loc[final_df["punt_return_PR"] > 0, "punt_return_AVG"] = (
        final_df["punt_return_YDS"] / final_df["punt_return_PR"]
    )
    final_df = final_df.reindex(columns=columns)

    final_df = final_df.round(
        {
            "passing_COMP%": 4,
            "passing_Y/A": 3,
            "passing_AY/A": 3,
            "passing_Y/C": 3,
            "passing_NFL_QBR": 3,
            "passing_CFB_QBR": 3,
            "rushing_AVG": 3,
            "kicking_FG%": 4,
            "punting_GROSS_AVG": 3,
            "punting_IN_20%": 4,
            "punting_TB%": 4,
            "kick_return_AVG": 3,
            "punt_return_AVG": 3,
        }
    )
    final_df = final_df.reindex(columns=columns)

    final_df["last_updated"] = datetime.now(UTC).isoformat()
    print(final_df)
    final_df.to_csv(
        f"season_stats/team/{season}_ufl_team_season_stats.csv",
        index=False
    )
    final_df.to_parquet(
        f"season_stats/team/{season}_ufl_team_season_stats.parquet",
        index=False
    )


if __name__ == "__main__":
    current_year = datetime.now().year
    try:
        mkdir("season_stats/player")
    except Exception as e:
        logging.warning(f"Unhandled exception {e}")

    try:
        mkdir("season_stats/team")
    except Exception as e:
        logging.warning(f"Unhandled exception {e}")

    parse_ufl_player_season_stats(current_year)
    parse_ufl_team_season_stats(current_year)
