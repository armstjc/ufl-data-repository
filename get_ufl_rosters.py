"""
# Creation Date: 03/30/2024 10:01 AM EDT
# Last Updated Date: 03/30/2024 10:01 AM EDT
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
    columns_order = [
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36",
        "Referer": "https://www.theufl.com/",
    }
    now = datetime.now(UTC).isoformat()
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
    
    for t_id in range(1,9):
        pass

    roster_df = roster_df[columns_order]
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
        "--save_csv",
        default=False,
        action=BooleanOptionalAction
    )
    parser.add_argument(
        "--save_parquet",
        default=False,
        action=BooleanOptionalAction
    )

    args = parser.parse_args()

    ufl_roster_data(
        season=now.year,
        save_csv=args.save_csv,
        save_parquet=args.save_parquet
    )
