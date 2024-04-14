"""
# Creation Date: 04/01/2024 03:00 PM EDT
# Last Updated Date: 04/14/2024 05:55 PM EDT
# Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
# File Name: get_ufl_schedules.py
# Purpose: Allows one to get UFL play-by-play (PBP) data.
###############################################################################
"""

import json
import logging
import re
from argparse import ArgumentParser, BooleanOptionalAction
from datetime import UTC, datetime

# from os import mkdir
# import time
import pandas as pd
import requests
from tqdm import tqdm

from utils import get_fox_api_key


def get_yardline(yardline, posteam):
    """ """
    try:
        yardline_temp = re.findall(
            "([0-9]+)",
            yardline
        )[0]
        
    except Exception as e:
        logging.info(
            f"Cannot get a yardline number with {yardline}." +
            f"Full exception {e}"
        )
        yardline_100 = yardline

    if (posteam in yardline) and ("end zone" in yardline.lower()):
        yardline_100 = 100
    elif (posteam not in yardline) and ("end zone" in yardline.lower()):
        yardline_100 = 0
    elif posteam in yardline:
        yardline_temp = int(yardline_temp)
        yardline_100 = 100 - yardline_temp
    else:
        yardline_temp = int(yardline_temp)
        yardline_100 = 100 - (50 - yardline_temp) - 50

    return yardline_100


def get_ufl_pbp(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False,
    # save_json: bool = True,
):
    """
    Retrieves UFL play-by-play (PBP) data,
    and parses them into a pandas `DataFrame`.

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

    Returns
    ----------
    A pandas `DataFrame` object with UFL PBP data.

    """
    fox_key = get_fox_api_key()
    columns_order = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/83.0.4103.97 Safari/537.36",
        # "Referer": "https://www.theufl.com/",
    }
    now = datetime.now(UTC).isoformat()
    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    schedule_df = pd.read_parquet(
        "https://github.com/armstjc/ufl-data-repository/releases/download/" +
        f"ufl-schedule/{season}_ufl_schedule.parquet"
    )

    schedule_df = schedule_df[
        (schedule_df["away_score"] > -1) | (schedule_df["home_score"] > -1)
    ]
    # print(schedule_df)

    ufl_game_id_arr = schedule_df["ufl_game_id"].to_numpy()
    ufl_game_type_arr = schedule_df["season_type"].to_numpy()
    week_title_arr = schedule_df["week_title"].to_numpy()

    for g in tqdm(range(0, len(ufl_game_id_arr))):
        ufl_game_id = ufl_game_id_arr[g]

        plays_df = pd.DataFrame()
        plays_arr = []
        url = (
            "https://api.foxsports.com/bifrost/v1/ufl/event/"
            + f"{ufl_game_id}/data?apikey={fox_key}"
        )
        # play_id = 0

        response = requests.get(url=url, headers=headers)
        game_json = json.loads(response.text)

        # Static data

        away_team_id = int(
            game_json["header"]["leftTeam"]["entityLink"]["layout"]["tokens"]["id"]
        )
        away_team_abv = game_json["header"]["leftTeam"]["name"]

        home_team_id = int(
            game_json["header"]["rightTeam"]["entityLink"]["layout"]["tokens"]["id"]
        )
        home_team_abv = game_json["header"]["rightTeam"]["name"]

        season_type = ufl_game_type_arr[g]
        week = int(week_title_arr[g].lower().replace("week ", ""))
        game_id = f"{season}_{week:02d}_{away_team_abv}_{home_team_abv}"

        stadium = game_json["header"]["venueName"]
        game_datetime = game_json["header"]["eventTime"]

        # Volatile data
        posteam = ""
        posteam_type = ""
        defteam = ""

        game_half = ""
        quarter_num = 0
        half_num = 0

        total_away_score = 0
        total_home_score = 0

        away_timeouts_remaining = 3
        home_timeouts_remaining = 3

        quarter_seconds_remaining = 900
        half_seconds_remaining = 1800
        game_seconds_remaining = 3600

        home_opening_kickoff = 0

        for quarter in game_json["pbp"]["sections"]:
            if quarter["title"] == "1ST QUARTER":
                quarter_num = 1
                half_num = 1
                game_half = "Half1"
            elif quarter["title"] == "2ND QUARTER":
                quarter_num = 2
                half_num = 1
                game_half = "Half1"
            elif quarter["title"] == "3RD QUARTER":
                quarter_num = 3
                half_num = 2
                game_half = "Half1"
                away_timeouts_remaining = 3
                home_timeouts_remaining = 3
            elif quarter["title"] == "4TH QUARTER":
                quarter_num = 4
                half_num = 2
                game_half = "Half1"
            elif quarter["title"] == "OVERTIME":
                quarter_num = 5
                half_num = 3
                game_half = "Overtime"
            else:
                q_title = quarter["title"]
                raise ValueError(f"Unhandled quarter name {q_title}")

            for drive in quarter["groups"]:
                # Variables that have to be cleared every drive
                is_drive_inside20 = 0
                posteam_score = 0
                defteam_score = 0
                score_differential = 0
                posteam_score_post = 0
                defteam_score_post = 0
                score_differential_post = 0
                posteam_timeouts_remaining = 0
                defteam_timeouts_remaining = 0
                drive_first_downs = 0
                drive_ended_with_score = 0

                drive_id = drive["id"]
                fixed_drive_result = drive["title"]
                drive_desc = drive["subtitle"]
                drive_desc = drive_desc.split(" · ")
                print(drive_desc)
                drive_play_count = int(drive_desc[0].replace(" plays", ""))
                drive_yards = int(drive_desc[1].replace(" yards", ""))
                drive_time_of_possession = drive_desc[2]
                drive_min, drive_sec = drive_time_of_possession.split(":")

                pos_team_temp = int(
                    drive["entityLink"]["layout"]["tokens"]["id"]
                )
                if pos_team_temp == away_team_id:
                    posteam = away_team_abv
                    defteam = home_team_abv
                    posteam_type = "away"
                    posteam_timeouts_remaining = away_timeouts_remaining
                    defteam_timeouts_remaining = home_timeouts_remaining
                elif pos_team_temp == home_team_id:
                    posteam = home_team_abv
                    defteam = away_team_abv
                    posteam_type = "home"
                    posteam_timeouts_remaining = home_timeouts_remaining
                    defteam_timeouts_remaining = away_timeouts_remaining
                else:
                    ValueError(f"Unhandled team ID {pos_team_temp}")

                if posteam == away_team_abv:
                    posteam_score = total_away_score
                    defteam_score = total_home_score
                elif posteam == home_team_abv:
                    posteam_score = total_home_score
                    defteam_score = total_away_score

                # print(drive_min,drive_sec)
                for play in drive["plays"]:
                    # print(play)

                    score_differential = posteam_score_post - defteam_score_post
                    posteam_score = posteam_score_post
                    defteam_score = defteam_score_post
                    # Variables that have to be cleared every play.
                    quarter_end = 0
                    is_goal_to_go = 0
                    is_shotgun = 0
                    is_qb_dropback = 0
                    is_qb_kneel = 0
                    is_qb_spike = 0
                    is_qb_scramble = 0
                    is_out_of_bounds = 0
                    is_scoring_play = 0
                    is_pass_play = 0
                    is_rush_play = 0
                    drive_quarter_start = 0
                    is_sack = 0
                    is_interception = 0
                    is_fumble = 0
                    is_penalty = 0
                    is_first_down = 0
                    kick_distance = 0
                    is_punt_attempt = 0
                    is_punt_blocked = 0
                    is_touchback = 0
                    no_huddle = 0
                    is_one_point_attempt = 0
                    is_two_point_attempt = 0
                    is_three_point_attempt = 0
                    one_point_conv_result = 0
                    two_point_conv_result = 0
                    three_point_conv_result = 0
                    is_aborted_play = 0
                    is_timeout = 0
                    yards_gained = 0
                    air_yards = 0
                    yards_after_catch = 0
                    is_pass_attempt = 0
                    is_rush_attempt = 0
                    is_touchdown = 0
                    is_safety = 0
                    is_incomplete_pass = 0
                    is_punt_downed = 0
                    is_punt_fair_catch = 0
                    is_punt_inside_twenty = 0
                    is_punt_in_endzone = 0
                    is_punt_out_of_bounds = 0
                    is_qb_hit = 0
                    is_pass_touchdown = 0
                    is_complete_pass = 0
                    is_field_goal_attempt = 0
                    return_yards = 0
                    is_solo_tackle = 0
                    is_assist_tackle = 0
                    tackle_with_assist = 0

                    run_location = ""
                    run_gap = ""
                    pass_length = ""
                    pass_location = ""
                    kicker_player_name = ""
                    field_goal_result = ""
                    timeout_team = ""
                    passer_player_name = ""
                    receiver_player_name = ""
                    td_team = ""
                    td_player_name = ""
                    punter_player_name = ""
                    punt_returner_player_name = ""
                    blocked_player_name = ""
                    solo_tackle_1_team = ""
                    assist_tackle_1_team = ""
                    assist_tackle_2_team = ""

                    play_id = int(play["id"])
                    try:
                        down_and_distance_temp = play["title"]
                    except Exception:
                        down_and_distance_temp = None
                    down = 0
                    yds_to_go = 0

                    try:
                        down, yds_to_go = \
                            down_and_distance_temp.lower().split(" and ")
                    except Exception as e:
                        logging.info(
                            "Could not parse down and distance" +
                            f"\nreason: {e}"
                        )
                        down = 0
                        yds_to_go = 0

                    try:
                        yrdln = play["subtitle"]
                    except Exception:
                        yrdln = None

                    time = play["timeOfPlay"]

                    desc = play["playDescription"]

                    if yrdln is not None:
                        side_of_field = re.findall(
                            "([a-zA-Z]+)",
                            yrdln
                        )
                        yardline_100 = get_yardline(
                            yrdln,
                            posteam
                        )

                        if yardline_100 <= 20:
                            is_drive_inside20 = 1
                    else:
                        side_of_field = None
                        yardline_100 = None

                    posteam_score_post
                    defteam_score_post
                    score_differential_post

                    # Quarter number
                    temp_quarter = ""
                    temp_quarter_num = 0
                    temp_half_num = 0
                    temp_game_half = ""

                    try:
                        temp_quarter = play["periodOfPlay"]
                        temp_quarter_num = int(temp_quarter[0])
                    except Exception:
                        logging.info("No temp quarter found.")

                    drive_quarter_start = quarter_num

                    if temp_quarter_num != 0:
                        drive_quarter_start = temp_quarter_num

                    del temp_quarter, temp_quarter_num
                    del temp_half_num, temp_game_half

                    if yardline_100 == yds_to_go:
                        # This is auto-set to `0`,
                        # unless it's an actual goal to go situation.
                        is_goal_to_go = 1

                    # Play type
                    if "no play" in desc.lower():
                        play_type = "no_play"

                    elif "tv timeout" in desc.lower():
                        play_type = "no_play"
                    elif "timeout #" in desc.lower():
                        play_type = "no_play"
                    elif "pass" in desc.lower():
                        play_type = "pass"
                        is_qb_dropback = 1
                        is_pass_play = 1
                        is_pass_attempt = 1
                    elif "spike" in desc.lower():
                        play_type = "qb_spike"
                        is_qb_dropback = 1
                        is_qb_spike = 1
                        is_pass_play = 1
                        is_pass_attempt = 1
                    elif "rushed" in desc.lower():
                        play_type = "run"
                        is_rush_play = 1
                        is_rush_attempt = 1
                    elif "scramble" in desc.lower():
                        play_type = "run"
                        is_qb_dropback = 1
                        is_qb_scramble = 1
                        is_rush_play = 1
                        is_rush_attempt = 1
                    elif "kneel" in desc.lower():
                        play_type = "qb_kneel"
                        is_qb_kneel = 1
                        is_rush_play = 1
                        is_rush_attempt = 1
                    elif "field goal" in desc.lower():
                        play_type = "field_goal"
                    elif "kickoff" in desc.lower() \
                            or "KICKOFF" in down_and_distance_temp.lower():
                        play_type = "kickoff"
                    elif "kicks" in desc.lower():
                        play_type = "punt"
                    elif "punt" in desc.lower():
                        play_type = "punt"
                    elif "end quarter" in desc.lower():
                        play_type = ""
                    elif "two minute warning." == desc.lower():
                        play_type = ""
                    elif "end game" == desc.lower():
                        play_type = ""
                    elif "(aborted)" in desc.lower():
                        play_type = "run"
                        is_aborted_play = 1
                    else:
                        raise ValueError(
                            f"Unhandled play `{desc}`"
                        )

                    # Run location
                    if play_type == "run" and "middle" in desc.lower():
                        run_location = "middle"
                        run_gap = ""
                    elif play_type == "run" and "right end" in desc.lower():
                        run_location = "right"
                        run_gap = "end"
                    elif play_type == "run" and "right guard" in desc.lower():
                        run_location = "right"
                        run_gap = "guard"
                    elif play_type == "run" and "right tackle" in desc.lower():
                        run_location = "right"
                        run_gap = "tackle"
                    elif play_type == "run" and "left end" in desc.lower():
                        run_location = "left"
                        run_gap = "end"
                    elif play_type == "run" and "left guard" in desc.lower():
                        run_location = "left"
                        run_gap = "guard"
                    elif play_type == "run" and "left tackle" in desc.lower():
                        run_location = "left"
                        run_gap = "tackle"
                    elif play_type == "run" and "left" in desc.lower():
                        run_location = "left"
                        run_gap = ""
                    elif play_type == "run" and "right" in desc.lower():
                        run_location = "right"
                        run_gap = ""
                    elif play_type == "run" and "fumble" in desc.lower():
                        is_fumble = 1
                    elif play_type == "run":
                        raise ValueError(
                            f"Unhandled play {desc}"
                        )

                    # Pass location
                    if play_type == "pass" and "deep left" in desc.lower():
                        pass_length = "deep"
                        pass_location = "left"
                    elif play_type == "pass" and "deep middle" in desc.lower():
                        pass_length = "deep"
                        pass_location = "left"
                    elif play_type == "pass" and "deep right" in desc.lower():
                        pass_length = "deep"
                        pass_location = "left"
                    elif play_type == "pass" and "short left" in desc.lower():
                        pass_length = "short"
                        pass_location = "left"
                    elif play_type == "pass" \
                            and "short middle" in desc.lower():
                        pass_length = "short"
                        pass_location = "left"
                    elif play_type == "pass" and "short right" in desc.lower():
                        pass_length = "short"
                        pass_location = "left"
                    elif play_type == "pass" and "left" in desc.lower():
                        pass_length = ""
                        pass_location = "left"
                    elif play_type == "pass" and "right" in desc.lower():
                        pass_length = ""
                        pass_location = "right"
                    elif play_type == "pass" and "sack" in desc.lower():
                        is_sack = 1
                    elif play_type == "pass" and "intercepted" in desc.lower():
                        is_interception = 1
                    elif play_type == "pass" \
                            and "-point attempt" in desc.lower():
                        pass_length = ""
                        pass_location = ""
                    elif play_type == "pass" and "penalty" in desc.lower():
                        is_penalty = 1
                    elif play_type == "pass":
                        raise ValueError(
                            f"Unhandled play {desc}"
                        )

                    if "shotgun" in desc.lower():
                        is_shotgun = 1

                    if "out of bounds" in desc.lower():
                        is_out_of_bounds = 1

                    if play["leftTeamScoreChange"] is True \
                            or play["rightTeamScoreChange"] is True:
                        is_scoring_play = 1

                    if play["leftTeamScoreChange"] is True \
                            and posteam == away_team_abv \
                            and "touchdown" in desc.lower():
                        drive_ended_with_score = 1
                        td_team = posteam
                    elif play["leftTeamScoreChange"] is True \
                            and posteam != away_team_abv \
                            and "touchdown" in desc.lower():
                        td_team = defteam
                    elif play["rightTeamScoreChange"] is True \
                            and posteam == home_team_abv \
                            and "touchdown" in desc.lower():
                        td_team = posteam
                        drive_ended_with_score = 1
                    elif play["rightTeamScoreChange"] is True \
                            and posteam != home_team_abv \
                            and "touchdown" in desc.lower():
                        td_team = defteam

                    if "first down" in desc.lower():
                        is_first_down = 1
                        drive_first_downs += 1

                    if "punt" in desc.lower():
                        is_punt_attempt = 1
                    elif "punt" in desc.lower() and "block" in desc.lower():
                        is_punt_attempt = 1

                    if "touchback" in desc.lower():
                        is_touchback = 1

                    if "no huddle" in desc.lower():
                        no_huddle = 1

                    if "qb hit" in desc.lower():
                        is_qb_hit = 1

                    if "field goal" in desc.lower():
                        is_field_goal_attempt = 1
                        check = re.findall(
                                r"([a-zA-Z]+\.[a-zA-Z]+) (\d\d) " +
                                r"yard field goal attempt is ([a-zA-Z\s]+),",
                                desc
                            )
                        kicker_player_name = check[0][0]
                        kick_distance = int(check[0][1])
                        field_goal_result = check[0][2].lower()
                        if field_goal_result == "good":
                            drive_ended_with_score = 1
                        del check

                    if "one-point conversion" in desc.lower():
                        is_one_point_attempt = 1
                        check = re.findall(
                            r"ONE-POINT ATTEMPT ([a-zA-Z]+).",
                            desc
                        )
                        one_point_conv_result = check[0]
                        del check
                    elif "two-point conversion" in desc.lower():
                        is_two_point_attempt = 1
                        check = re.findall(
                            r"TWO-POINT ATTEMPT ([a-zA-Z]+).",
                            desc
                        )
                        two_point_conv_result = check[0]
                        del check
                    elif "three-point conversion" in desc.lower():
                        is_three_point_attempt = 1
                        check = re.findall(
                            r"THREE-POINT ATTEMPT ([a-zA-Z]+).",
                            desc
                        )
                        three_point_conv_result = check[0]
                        del check

                    if "timeout #" in desc.lower():
                        is_timeout = 1
                        check = re.findall(
                            r"Timeout #([0-9]+) by ([a-zA-Z]+)",
                            desc
                        )
                        # timeout_num = int(check[0][0])
                        timeout_team = check[0][1]

                        if timeout_team == home_team_abv:
                            home_timeouts_remaining -= 1
                        else:
                            away_timeouts_remaining -= 1

                        if timeout_team == posteam:
                            posteam_timeouts_remaining -= 1
                        else:
                            defteam_timeouts_remaining -= 1

                        del check
                        # del timeout_num

                    if "pass incomplete intended for" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-]+) pass incomplete " +
                            r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        receiver_player_name = check[0][1]
                        is_incomplete_pass = 1

                        del check

                    elif "pass" in desc.lower() and "incomplete" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-]+) pass incomplete " +
                            r"([a-zA-Z]+) ([a-zA-Z]+)? " +
                            r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        receiver_player_name = check[0][3]
                        is_incomplete_pass = 1

                        del check

                    elif "pass complete" in desc.lower():
                        is_complete_pass = 1
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass complete to " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Catch made by ([a-zA-Z]\.[a-zA-Z\'\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Gain of( -?[0-9]+)? yards",
                            desc
                        )
                        passer_player_name = check[0][0]
                        pass_yardline_temp = check[0][1]
                        receiver_player_name = check[0][2]
                        yards_gained = check[0][4]
                        pass_yardline_num = get_yardline(
                            pass_yardline_temp,
                            posteam
                        )

                        if yards_gained == '':
                            air_yards = 0
                            yards_after_catch = 0
                            passing_yards = 0
                        else:
                            yards_gained = int(yards_gained)
                            air_yards = pass_yardline_num - yardline_100
                            yards_after_catch = yardline_100 - (
                                yardline_100 - yards_gained
                            )
                            passing_yards = yards_gained

                        if "touchdown" in desc.lower():
                            is_pass_touchdown = 1

                        del check

                    elif "pass" in desc.lower() and "complete" in desc.lower():
                        is_complete_pass = 1
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass " +
                            r"([a-zA-Z]+) ([a-zA-Z]+) complete to " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Catch made by ([a-zA-Z]\.[a-zA-Z\'\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Gain of( -?[0-9]+)? yards",
                            desc
                        )
                        passer_player_name = check[0][0]
                        pass_yardline_temp = check[0][3]
                        receiver_player_name = check[0][4]
                        yards_gained = check[0][6]

                        pass_yardline_num = get_yardline(
                            pass_yardline_temp,
                            posteam
                        )

                        del pass_yardline_temp

                        if yards_gained == '':
                            air_yards = 0
                            yards_after_catch = 0
                            passing_yards = 0
                            receiving_yards = passing_yards
                        else:
                            yards_gained = int(yards_gained)
                            air_yards = pass_yardline_num - yardline_100
                            yards_after_catch = yardline_100 - (
                                yardline_100 - yards_gained
                            )
                            passing_yards = yards_gained
                            receiving_yards = passing_yards

                        if "touchdown" in desc.lower():
                            is_pass_touchdown = 1

                        del check

                    if "touchdown" in desc.lower():
                        is_touchdown = 1
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-]+) " +
                            r"for( [0-9]+)? yards, TOUCHDOWN.",
                            desc
                        )
                        td_player_name = check[0][0]

                        del check

                    if "safety" in desc.lower():
                        is_safety = 1

                    if "punts yards" in desc.lower():
                        # Yes, there is a play where it's written
                        # "{player} punts yards to {yardline}"
                        # It appears to be an edge case when
                        # someone punts twice in one play.
                        # This is to catch that edge case.
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-]+) punts " +
                            r"yards to ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)",
                            desc
                        )
                        is_punt_attempt = 1
                        punter_player_name = check[0][0]
                        # kick_distance = int(check[0][1])
                        kick_to_temp = check[0][1]
                        kick_to_yd_line = get_yardline(
                            kick_to_temp,
                            posteam
                        )

                        del check

                    elif "punts" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-]+) punts ([0-9\-]+) " +
                            r"yards to ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)",
                            desc
                        )
                        is_punt_attempt = 1
                        punter_player_name = check[0][0]
                        kick_distance = int(check[0][1])
                        kick_to_temp = check[0][2]
                        kick_to_yd_line = get_yardline(
                            kick_to_temp,
                            posteam
                        )

                        if "downed" in desc.lower():
                            is_punt_downed = 1
                        elif "downed" in desc.lower() and kick_to_yd_line < 20:
                            is_punt_downed = 1
                            is_punt_inside_twenty = 1

                        if "fair catch" in desc.lower() and kick_to_yd_line < 20:
                            check = re.findall(
                                r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-]+).",
                                desc
                            )
                            punt_returner_player_name = check[0][0]
                            is_punt_fair_catch = 1
                            del check
                        elif "fair catch" in desc.lower():
                            check = re.findall(
                                r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-]+).",
                                desc
                            )
                            punt_returner_player_name = check[0][0]
                            is_punt_fair_catch = 1
                            del check
                            is_punt_inside_twenty = 1

                        if "blocked" in desc.lower():
                            check = re.findall(
                                r"\. ([a-zA-Z\.\'\-\s\;]+)? blocked the kick.",
                                desc
                            )
                            is_punt_blocked = 1
                            blocked_player_name = check[0]
                            print()

                        if "touchback" in desc.lower():
                            is_punt_in_endzone = 1

                        if "out of bounds" in desc.lower():
                            is_punt_out_of_bounds = 1
                        elif "out of bounds" in desc.lower():
                            is_punt_out_of_bounds = 1
                            is_punt_inside_twenty = 1

                        if "tackled by" in desc.lower():
                            check = re.findall(
                                r"([a-zA-Z]\.[a-zA-Z\'\-]+) " +
                                r"returned punt from " +
                                r"the ([a-zA-Z]+\s[0-9]+|" +
                                r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                                r"Tackled by " +
                                r"([a-zA-Z]\.[a-zA-Z\'\-\s\,\.\;]+) at " +
                                r"([a-zA-Z]+\s[0-9]+|" +
                                r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                                desc
                            )
                            punt_returner_player_name = check[0][0]
                            punt_return_start_temp = check[0][1]
                            tacklers_temp = check[0][2]
                            punt_return_end_temp = check[0][3]

                            punt_return_start_num = get_yardline(
                                punt_return_start_temp,
                                posteam
                            )

                            punt_return_end_num = get_yardline(
                                punt_return_end_temp,
                                posteam
                            )
                            return_yards = punt_return_end_num - \
                                punt_return_start_num

                            if ";" in tacklers_temp:
                                is_assist_tackle = 1
                                tackle_with_assist = 1
                                assist_tackle_1_team = posteam
                                assist_tackle_2_team = posteam
                            if "," in tacklers_temp:
                                pass
                            else:
                                is_solo_tackle = 1
                                solo_tackle_1_team = posteam
                                solo_tackle_1_player_name = tacklers_temp

                            print()

                        is_punt_inside_twenty = 1

                        del check

                    # if temp_quarter_num == 1:
                    #     # temp_quarter_num = 1
                    #     temp_half_num = 1
                    #     temp_game_half = "Half1"
                    # elif temp_quarter_num == 2:
                    #     # temp_quarter_num = 1
                    #     temp_half_num = 1
                    #     temp_game_half = "Half1"
                    # elif temp_quarter_num == 3:
                    #     # temp_quarter_num = 1
                    #     temp_half_num = 2
                    #     temp_game_half = "Half2"
                    # elif temp_quarter_num == 4:
                    #     # temp_quarter_num = 0
                    #     temp_half_num = 2
                    #     temp_game_half = "Half2"

                    temp_df = pd.DataFrame(
                        {
                            "play_id": play_id,
                            "ufl_game_id": ufl_game_id,
                            "game_id": game_id,
                            "home_team": home_team_abv,
                            "away_team": away_team_abv,
                            "season_type": season_type,
                            "week": week,
                            "posteam": posteam,
                            "posteam_type": posteam_type,
                            "defteam": defteam,
                            "side_of_field": side_of_field,
                            "yardline_100": yardline_100,
                            "game_date": game_datetime,
                            "quarter_seconds_remaining":
                            quarter_seconds_remaining,
                            "half_seconds_remaining": half_seconds_remaining,
                            "game_seconds_remaining": game_seconds_remaining,
                            "game_half": game_half,
                            "drive": drive_id,
                            "sp": is_scoring_play,
                            "qtr": quarter_num,
                            "down": down,
                            "goal_to_go": is_goal_to_go,
                            "time": time,
                            "yrdln": yrdln,
                            "ydstogo": yds_to_go,
                            # "ydsnet": yds_net,
                            "desc": desc,
                            "play_type": play_type,
                            "yards_gained": yards_gained,
                            "shotgun": is_shotgun,
                            "no_huddle": no_huddle,
                            "qb_dropback": is_qb_dropback,
                            "qb_kneel": is_qb_kneel,
                            "qb_spike": is_qb_spike,
                            "qb_scramble": is_qb_scramble,
                            "pass_length": pass_length,
                            "pass_location": pass_location,
                            "air_yards": air_yards,
                            "yards_after_catch": yards_after_catch,
                            "run_location": run_location,
                            "run_gap": run_gap,
                            "field_goal_result": field_goal_result,
                            "kick_distance": kick_distance,
                            # "extra_point_result": extra_point_result,
                            "one_point_conv_result": one_point_conv_result,
                            "two_point_conv_result": two_point_conv_result,
                            "three_point_conv_result": three_point_conv_result,
                            "home_timeouts_remaining": home_timeouts_remaining,
                            "away_timeouts_remaining": away_timeouts_remaining,
                            "timeout": is_timeout,
                            "timeout_team": timeout_team,
                            "td_team": td_team,
                            "td_player_name": td_player_name,
                            # "td_player_id": td_player_id,
                            "posteam_timeouts_remaining":
                            posteam_timeouts_remaining,
                            "defteam_timeouts_remaining":
                            defteam_timeouts_remaining,
                            "total_home_score": total_home_score,
                            "total_away_score": total_away_score,
                            "posteam_score": posteam_score,
                            "defteam_score": defteam_score,
                            "score_differential": score_differential,
                            "posteam_score_post": posteam_score_post,
                            "defteam_score_post": defteam_score_post,
                            "score_differential_post": score_differential_post,
                            "punt_blocked": is_punt_blocked,
                            "first_down_rush": first_down_rush,
                            "first_down_pass": first_down_pass,
                            "first_down_penalty": first_down_penalty,
                            "third_down_converted": third_down_converted,
                            "third_down_failed": third_down_failed,
                            "fourth_down_converted": fourth_down_converted,
                            "fourth_down_failed": fourth_down_failed,
                            "incomplete_pass": is_incomplete_pass,
                            "touchback": is_touchback,
                            "interception": is_interception,
                            "punt_inside_twenty": is_punt_inside_twenty,
                            "punt_in_endzone": is_punt_in_endzone,
                            "punt_out_of_bounds": is_punt_out_of_bounds,
                            "punt_downed": is_punt_downed,
                            "punt_fair_catch": is_punt_fair_catch,
                            "kickoff_inside_twenty": is_kickoff_inside_twenty,
                            "kickoff_in_endzone": is_kickoff_in_endzone,
                            "kickoff_out_of_bounds": is_kickoff_out_of_bounds,
                            "kickoff_downed": is_kickoff_downed,
                            "kickoff_fair_catch": is_kickoff_fair_catch,
                            "fumble_forced": is_fumble_forced,
                            "fumble_not_forced": is_fumble_not_forced,
                            "fumble_out_of_bounds": is_fumble_out_of_bounds,
                            "solo_tackle": is_solo_tackle,
                            "safety": is_safety,
                            "penalty": is_penalty,
                            "tackled_for_loss": is_tackled_for_loss,
                            "fumble_lost": is_fumble_lost,
                            "own_kickoff_recovery": is_own_kickoff_recovery,
                            "own_kickoff_recovery_td": is_own_kickoff_recovery_td,
                            "qb_hit": is_qb_hit,
                            "rush_attempt": is_rush_attempt,
                            "pass_attempt": is_pass_attempt,
                            "sack": is_sack,
                            "touchdown": is_touchdown,
                            "pass_touchdown": is_pass_touchdown,
                            "rush_touchdown": is_rush_touchdown,
                            "return_touchdown": is_return_touchdown,
                            # "extra_point_attempt": is_extra_point_attempt,
                            "one_point_attempt": is_one_point_attempt,
                            "two_point_attempt": is_two_point_attempt,
                            "three_point_attempt": is_three_point_attempt,
                            "field_goal_attempt": is_field_goal_attempt,
                            "kickoff_attempt": is_kickoff_attempt,
                            "punt_attempt": is_punt_attempt,
                            "fumble": is_fumble,
                            "complete_pass": is_complete_pass,
                            "assist_tackle": is_assist_tackle,
                            "lateral_reception": lateral_reception,
                            "lateral_rush": lateral_rush,
                            "lateral_return": lateral_return,
                            "lateral_recovery": lateral_recovery,
                            # "passer_player_id": passer_player_id,
                            "passer_player_name": passer_player_name,
                            "passing_yards": passing_yards,
                            # "receiver_player_id": receiver_player_id,
                            "receiver_player_name": receiver_player_name,
                            "receiving_yards": receiving_yards,
                            # "rusher_player_id": rusher_player_id,
                            "rusher_player_name": rusher_player_name,
                            "rushing_yards": rushing_yards,
                            # "lateral_receiver_player_id":
                            # lateral_receiver_player_id,
                            "lateral_receiver_player_name": lateral_receiver_player_name,
                            "lateral_receiving_yards": lateral_receiving_yards,
                            # "lateral_rusher_player_id":
                            # lateral_rusher_player_id,
                            "lateral_rusher_player_name": lateral_rusher_player_name,
                            "lateral_rushing_yards": lateral_rushing_yards,
                            # "lateral_sack_player_id": lateral_sack_player_id,
                            "lateral_sack_player_name": lateral_sack_player_name,
                            # "interception_player_id": interception_player_id,
                            "interception_player_name": interception_player_name,
                            # "lateral_interception_player_id":
                            # lateral_interception_player_id,
                            "lateral_interception_player_name": lateral_interception_player_name,
                            # "punt_returner_player_id":
                            # punt_returner_player_id,
                            "punt_returner_player_name":
                            punt_returner_player_name,
                            # "lateral_punt_returner_player_id":
                            # lateral_punt_returner_player_id,
                            # "lateral_punt_returner_player_name":
                            # lateral_punt_returner_player_name,
                            "kickoff_returner_player_name": kickoff_returner_player_name,
                            # "kickoff_returner_player_id":
                            # kickoff_returner_player_id,
                            # "lateral_kickoff_returner_player_id":
                            # lateral_kickoff_returner_player_id,
                            "lateral_kickoff_returner_player_name": lateral_kickoff_returner_player_name,
                            # "punter_player_id": punter_player_id,
                            "punter_player_name": punter_player_name,
                            "kicker_player_name": kicker_player_name,
                            # "kicker_player_id": kicker_player_id,
                            # "own_kickoff_recovery_player_id":
                            # own_kickoff_recovery_player_id,
                            "own_kickoff_recovery_player_name": own_kickoff_recovery_player_name,
                            # "blocked_player_id": blocked_player_id,
                            "blocked_player_name": blocked_player_name,
                            # "tackle_for_loss_1_player_id":
                            # tackle_for_loss_1_player_id,
                            "tackle_for_loss_1_player_name": tackle_for_loss_1_player_name,
                            # "tackle_for_loss_2_player_id":
                            # tackle_for_loss_2_player_id,
                            "tackle_for_loss_2_player_name": tackle_for_loss_2_player_name,
                            # "qb_hit_1_player_id": qb_hit_1_player_id,
                            # "qb_hit_1_player_name": qb_hit_1_player_name,
                            # "qb_hit_2_player_id": qb_hit_2_player_id,
                            # "qb_hit_2_player_name": qb_hit_2_player_name,
                            "forced_fumble_player_1_team": forced_fumble_player_1_team,
                            # "forced_fumble_player_1_player_id":
                            # forced_fumble_player_1_player_id,
                            "forced_fumble_player_1_player_name": forced_fumble_player_1_player_name,
                            "forced_fumble_player_2_team": forced_fumble_player_2_team,
                            # "forced_fumble_player_2_player_id":
                            # forced_fumble_player_2_player_id,
                            "forced_fumble_player_2_player_name": forced_fumble_player_2_player_name,
                            "solo_tackle_1_team": solo_tackle_1_team,
                            # "solo_tackle_2_team": solo_tackle_2_team,
                            # "solo_tackle_1_player_id":
                            # solo_tackle_1_player_id,
                            # "solo_tackle_2_player_id":
                            # solo_tackle_2_player_id,
                            "solo_tackle_1_player_name": solo_tackle_1_player_name,
                            # "solo_tackle_2_player_name": solo_tackle_2_player_name,
                            # "assist_tackle_1_player_id":
                            # assist_tackle_1_player_id,
                            "assist_tackle_1_player_name": assist_tackle_1_player_name,
                            "assist_tackle_1_team": assist_tackle_1_team,
                            # "assist_tackle_2_player_id":
                            # assist_tackle_2_player_id,
                            "assist_tackle_2_player_name": assist_tackle_2_player_name,
                            "assist_tackle_2_team": assist_tackle_2_team,
                            # "assist_tackle_3_player_id":
                            # assist_tackle_3_player_id,
                            "assist_tackle_3_player_name": assist_tackle_3_player_name,
                            "assist_tackle_3_team": assist_tackle_3_team,
                            # "assist_tackle_4_player_id":
                            # assist_tackle_4_player_id,
                            "assist_tackle_4_player_name": assist_tackle_4_player_name,
                            "assist_tackle_4_team": assist_tackle_4_team,
                            "tackle_with_assist": tackle_with_assist,
                            # "tackle_with_assist_1_player_id":
                            # tackle_with_assist_1_player_id,
                            # "tackle_with_assist_1_player_name":
                            # tackle_with_assist_1_player_name,
                            # "tackle_with_assist_1_team":
                            # tackle_with_assist_1_team,
                            # "tackle_with_assist_2_player_id":
                            # tackle_with_assist_2_player_id,
                            # "tackle_with_assist_2_player_name":
                            # tackle_with_assist_2_player_name,
                            # "tackle_with_assist_2_team":
                            # tackle_with_assist_2_team,
                            # "pass_defense_1_player_id":
                            # pass_defense_1_player_id,
                            "pass_defense_1_player_name": pass_defense_1_player_name,
                            # "pass_defense_2_player_id":
                            # pass_defense_2_player_id,
                            "pass_defense_2_player_name": pass_defense_2_player_name,
                            "fumbled_1_team": fumbled_1_team,
                            # "fumbled_1_player_id":
                            # fumbled_1_player_id,
                            "fumbled_1_player_name": fumbled_1_player_name,
                            # "fumbled_2_player_id":
                            # fumbled_2_player_id,
                            "fumbled_2_player_name": fumbled_2_player_name,
                            "fumbled_2_team": fumbled_2_team,
                            "fumble_recovery_1_team": fumble_recovery_1_team,
                            "fumble_recovery_1_yards": fumble_recovery_1_yards,
                            # "fumble_recovery_1_player_id":
                            # fumble_recovery_1_player_id,
                            "fumble_recovery_1_player_name": fumble_recovery_1_player_name,
                            "fumble_recovery_2_team": fumble_recovery_2_team,
                            "fumble_recovery_2_yards": fumble_recovery_2_yards,
                            # "fumble_recovery_2_player_id":
                            # fumble_recovery_2_player_id,
                            "fumble_recovery_2_player_name": fumble_recovery_2_player_name,
                            "sack_player_id": sack_player_id,
                            "sack_player_name": sack_player_name,
                            # "half_sack_1_player_id":
                            # half_sack_1_player_id,
                            "half_sack_1_player_name": half_sack_1_player_name,
                            # "half_sack_2_player_id":
                            # half_sack_2_player_id,
                            "half_sack_2_player_name": half_sack_2_player_name,
                            "return_team": return_team,
                            "return_yards": return_yards,
                            "penalty_team": penalty_team,
                            # "penalty_player_id": penalty_player_id,
                            "penalty_player_name": penalty_player_name,
                            "penalty_yards": penalty_yards,
                            "replay_or_challenge": is_replay_or_challenge,
                            "replay_or_challenge_result": replay_or_challenge_result,
                            "penalty_type": penalty_type,
                            "defensive_two_point_attempt": defensive_two_point_attempt,
                            "defensive_two_point_conv": defensive_two_point_conv,
                            "defensive_extra_point_attempt": defensive_extra_point_attempt,
                            "defensive_extra_point_conv": defensive_extra_point_conv,
                            "safety_player_id": safety_player_id,
                            "safety_player_name": safety_player_name,
                            "season": season,
                            "series": series_id,
                            "series_success": series_success,
                            "series_result": series_result,
                            "order_sequence": order_sequence,
                            "start_time": None,
                            "time_of_day": None,
                            "stadium": stadium,
                            "play_clock": 0,
                            "play_deleted": 0,
                            # "play_type_nfl": play_type_nfl,
                            "special_teams_play": special_teams_play,
                            "st_play_type": None,
                            "end_clock_time": None,
                            "end_yard_line": None,
                            "fixed_drive": drive_id,
                            "fixed_drive_result": fixed_drive_result,
                            "drive_real_start_time": None,
                            "drive_play_count": drive_play_count,
                            "drive_time_of_possession":
                            drive_time_of_possession,
                            "drive_first_downs": drive_first_downs,
                            "drive_inside20": is_drive_inside20,
                            "drive_ended_with_score": drive_ended_with_score,
                            "drive_quarter_start": drive_quarter_start,
                            "drive_quarter_end": quarter,
                            "drive_yards_penalized": drive_yards_penalized,
                            "drive_start_transition": drive_start_transition,
                            "drive_end_transition": drive_end_transition,
                            "drive_game_clock_start": drive_game_clock_start,
                            "drive_game_clock_end": drive_game_clock_end,
                            "drive_start_yard_line": drive_start_yard_line,
                            "drive_end_yard_line": drive_end_yard_line,
                            "drive_play_id_started": drive_play_id_started,
                            "drive_play_id_ended": drive_play_id_ended,
                            "home_score": total_home_score,
                            "away_score": total_away_score,
                            "location": "Home",  # either "Home" or "Neutral"
                            "result": 0,
                            # Betting
                            "total": 0,
                            "spread_line": spread_line,
                            "total_line": total_line,
                            "div_game": is_divisional_game,
                            "roof": roof,
                            "surface": playing_surface,
                            "temp": game_temp,
                            "wind": game_wind,
                            "home_coach": home_coach,
                            "away_coach": away_coach,
                            "stadium_id": None,
                            "game_stadium": stadium,
                            "aborted_play": is_aborted_play,
                            "success": is_successful_play,
                            "passer": passer_player_name,
                            # "passer_jersey_number": passer_jersey_number,
                            "rusher": rusher_player_name,
                            # "rusher_jersey_number": rusher_jersey_number,
                            "receiver": receiver_player_name,
                            # "receiver_jersey_number": receiver_jersey_number,
                            "pass": is_pass_play,
                            "rush": is_rush_play,
                            "first_down": is_first_down,
                            "special": is_special_teams_play,
                            "play": is_scrimmage_play,
                            # "passer_id": passer_player_id,
                            # "rusher_id": rusher_player_id,
                            # "receiver_id": receiver_player_id,
                            "out_of_bounds": is_out_of_bounds,
                            "home_opening_kickoff": home_opening_kickoff,
                        },
                        index=[0],
                    )

            play_id += 1

        plays_df = pd.concat(plays_arr, ignore_index=True)

        plays_df["away_score"] = total_away_score
        plays_df["home_score"] = total_home_score
        plays_df["result"] = total_home_score - total_away_score
        plays_df["total"] = total_home_score + total_away_score


if __name__ == "__main__":
    now = datetime.now()

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

    get_ufl_pbp(
        season=now.year,
        save_csv=args.save_csv,
        save_parquet=args.save_parquet
    )
