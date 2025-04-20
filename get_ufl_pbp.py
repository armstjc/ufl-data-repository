"""
# Creation Date: 04/01/2024 03:00 PM EDT
# Last Updated Date: 05/17/2024 06:45 PM EDT
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
from glob import glob

# from os import mkdir
# import time
import pandas as pd
import requests
import numpy as np
from tqdm import tqdm

from utils import get_fox_api_key, format_folder_path


def get_yardline(yardline: str, posteam: str):
    """ """
    try:
        yardline_temp = re.findall("([0-9]+)", yardline)[0]
    except Exception as e:
        logging.info(
            f"Cannot get a yardline number with {yardline}." + f"Full exception {e}"
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


def parser(
    game_json: dict,
    ufl_game_id: int,
    season: int,
    season_type: str,
    week: int,
) -> pd.DataFrame:
    """ """
    # Static data
    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    temp_df = pd.DataFrame()

    away_team_id = int(
        game_json["header"]["leftTeam"]["entityLink"]["layout"]["tokens"]["id"]
    )
    away_team_abv = game_json["header"]["leftTeam"]["name"]

    home_team_id = int(
        game_json["header"]["rightTeam"]["entityLink"]["layout"]["tokens"]["id"]
    )
    home_team_abv = game_json["header"]["rightTeam"]["name"]

    game_id = f"{season}_{week:02d}_{away_team_abv}_{home_team_abv}"

    stadium = game_json["header"]["venueName"]
    game_datetime_str = game_json["header"]["eventTime"]
    game_datetime = datetime.strptime(game_datetime_str, "%Y-%m-%dT%H:%M:%SZ")
    # Volatile data
    play_id = 0
    drive_id = 0
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

            if " · " in drive_desc:
                drive_desc = drive_desc.split(" · ")
            elif " Â· " in drive_desc:
                drive_desc = drive_desc.split(" Â· ")
            print(drive_desc)
            drive_play_count = int(drive_desc[0].replace(" plays", ""))
            # drive_yards = int(drive_desc[1].replace(" yards", ""))
            drive_time_of_possession = drive_desc[2]
            drive_min, drive_sec = drive_time_of_possession.split(":")

            pos_team_temp = int(drive["entityLink"]["layout"]["tokens"]["id"])
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

            for play in drive["plays"]:
                play_id += 1
                tacklers_arr = []
                sack_players_arr = []
                score_differential = posteam_score_post - defteam_score_post
                posteam_score = posteam_score_post
                defteam_score = defteam_score_post

                play_id = int(play["id"])
                try:
                    down_and_distance_temp = play["title"]
                except Exception:
                    down_and_distance_temp = ""
                down = 0
                yds_to_go = 0
                try:
                    down, yds_to_go = down_and_distance_temp.lower().split(" and ")
                    down = int(down)
                    yds_to_go = int(yds_to_go)
                except Exception as e:
                    logging.info("Could not parse down and distance" + f"\nreason: {e}")
                    down = 0
                    yds_to_go = 0

                try:
                    yrdln = play["subtitle"]
                except Exception:
                    yrdln = None

                time = play["timeOfPlay"]
                time_min, time_sec = time.split(":")
                time_min = int(time_min)
                time_sec = int(time_sec)

                play_desc = play["playDescription"]

                # PBP text clean up
                # play_desc = play_desc.replace("[", "")
                # play_desc = play_desc.replace("]", "")
                play_desc = play_desc.replace(
                    "Face Mask (15 Yards),",
                    "Face Mask,"
                )
                play_desc = play_desc.replace(
                    " forced by. ",
                    " forced by TEAM. "
                )
                if yrdln is not None:
                    side_of_field = re.sub(r"([0-9\s]+)", r"", yrdln)
                    yardline_100 = get_yardline(yrdln, posteam)

                else:
                    side_of_field = "50"
                    yardline_100 = 50

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
                    play_quarter_num = temp_quarter_num
                else:
                    play_quarter_num = drive_quarter_start

                del temp_quarter, temp_quarter_num
                del temp_half_num, temp_game_half

                if half_num == 1:
                    quarter_seconds_remaining = (time_min * 60) + time_sec
                    half_seconds_remaining = (
                        ((2 - play_quarter_num) * 900) +
                        (time_min * 60) + time_sec
                    )
                    game_seconds_remaining = (
                        ((4 - play_quarter_num) * 900) +
                        (time_min * 60) + time_sec
                    )
                elif half_num == 2:
                    quarter_seconds_remaining = (time_min * 60) + time_sec
                    half_seconds_remaining = (
                        ((4 - play_quarter_num) * 900) +
                        (time_min * 60) + time_sec
                    )
                    game_seconds_remaining = (
                        ((4 - play_quarter_num) * 900) +
                        (time_min * 60) + time_sec
                    )
                elif half_num == 3:
                    quarter_seconds_remaining = 0
                    half_seconds_remaining = 0
                    game_seconds_remaining = 0

                temp_df = pd.DataFrame(
                    {
                        "season": game_datetime.year,
                        "play_id": play_id,
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
                        "game_date": game_datetime.strftime("%Y-%m-%d"),
                        "quarter_seconds_remaining": quarter_seconds_remaining,
                        "half_seconds_remaining": half_seconds_remaining,
                        "game_seconds_remaining": game_seconds_remaining,
                        "game_half": game_half,
                        "is_quarter_end": False,
                        "drive": drive_id,
                        "is_scoring_play": False,
                        "qtr": quarter_num,
                        "down": down,
                        "is_goal_to_go": False,
                        "time": time,
                        "yrdln": yrdln,
                        "ydstogo": yds_to_go,
                        "ydsnet": 0,
                        "desc": play_desc,
                        "play_type": None,
                        "yards_gained": 0,
                        "is_shotgun": False,
                        "is_no_huddle": False,
                        "is_qb_dropback": False,
                        "is_qb_kneel": False,
                        "is_qb_spike": False,
                        "is_qb_scramble": False,
                        "pass_length": None,
                        "pass_location": None,
                        "air_yards": None,
                        "yards_after_catch": None,
                        "run_location": None,
                        "run_gap": None,
                        "field_goal_result": None,
                        "kick_distance": None,
                        "extra_point_result": None,
                        "two_point_conv_result": None,
                        "home_timeouts_remaining": home_timeouts_remaining,
                        "away_timeouts_remaining": away_timeouts_remaining,
                        "is_timeout": False,
                        "timeout_team": None,
                        "td_team": None,
                        "td_player_name": None,
                        "td_player_id": None,
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
                        "score_differential_post": defteam_score_post,
                        "is_4th_and_12_onside_play": False,
                        "is_punt_blocked": False,
                        "is_first_down_rush": False,
                        "is_first_down_pass": False,
                        "is_first_down_penalty": False,
                        "is_third_down_converted": False,
                        "is_third_down_failed": False,
                        "is_fourth_down_converted": False,
                        "is_fourth_down_failed": False,
                        "is_incomplete_pass": False,
                        "is_touchback": False,
                        "is_interception": False,
                        "is_punt_inside_twenty": False,
                        "is_punt_in_endzone": False,
                        "is_punt_out_of_bounds": False,
                        "is_punt_downed": False,
                        "is_punt_fair_catch": False,
                        "is_kickoff_inside_twenty": False,
                        "is_kickoff_in_endzone": False,
                        "is_kickoff_out_of_bounds": False,
                        "is_kickoff_downed": False,
                        "is_kickoff_fair_catch": False,
                        "is_fumble_forced": False,
                        "is_fumble_not_forced": False,
                        "is_fumble_out_of_bounds": False,
                        "is_solo_tackle": False,
                        "is_safety": False,
                        "is_penalty": False,
                        "is_tackled_for_loss": False,
                        "is_fumble_lost": False,
                        "is_own_kickoff_recovery": False,
                        "is_own_kickoff_recovery_td": False,
                        "is_qb_hit": False,
                        "is_rush_attempt": False,
                        "is_pass_attempt": False,
                        "is_sack": False,
                        "is_touchdown": False,
                        "is_pass_touchdown": False,
                        "is_rush_touchdown": False,
                        "is_return_touchdown": False,
                        "is_extra_point_attempt": False,
                        "is_one_point_attempt": False,
                        "is_one_point_attempt_success": False,
                        "is_two_point_attempt": False,
                        "is_two_point_attempt_success": False,
                        "is_three_point_attempt": False,
                        "is_three_point_attempt_success": False,
                        "is_field_goal_attempt": False,
                        "is_kickoff_attempt": False,
                        "is_punt_attempt": False,
                        "is_fumble": False,
                        "is_complete_pass": False,
                        "is_assist_tackle": False,
                        "is_lateral_reception": False,
                        "is_lateral_rush": False,
                        "is_lateral_return": False,
                        "is_lateral_recovery": False,
                        "passer_player_id": None,
                        "passer_player_name": None,
                        "passing_yards": None,
                        "receiver_player_id": None,
                        "receiver_player_name": None,
                        "receiving_yards": None,
                        "rusher_player_id": None,
                        "rusher_player_name": None,
                        "rushing_yards": None,
                        "lateral_receiver_player_id": None,
                        "lateral_receiver_player_name": None,
                        "lateral_receiving_yards": None,
                        "lateral_rusher_player_id": None,
                        "lateral_rusher_player_name": None,
                        "lateral_rushing_yards": None,
                        "lateral_sack_player_id": None,
                        "lateral_sack_player_name": None,
                        "interception_player_id": None,
                        "interception_player_name": None,
                        "lateral_interception_player_id": None,
                        "lateral_interception_player_name": None,
                        "punt_returner_player_id": None,
                        "punt_returner_player_name": None,
                        "lateral_punt_returner_player_id": None,
                        "lateral_punt_returner_player_name": None,
                        "kickoff_returner_player_name": None,
                        "kickoff_returner_player_id": None,
                        "lateral_kickoff_returner_player_id": None,
                        "lateral_kickoff_returner_player_name": None,
                        "punter_player_id": None,
                        "punter_player_name": None,
                        "kicker_player_name": None,
                        "kicker_player_id": None,
                        "own_kickoff_recovery_player_id": None,
                        "own_kickoff_recovery_player_name": None,
                        "blocked_player_id": None,
                        "blocked_player_name": None,
                        "long_snapper_player_id": None,
                        "long_snapper_player_name": None,
                        "holder_player_id": None,
                        "holder_player_name": None,
                        "tackle_for_loss_1_player_id": None,
                        "tackle_for_loss_1_player_name": None,
                        "tackle_for_loss_2_player_id": None,
                        "tackle_for_loss_2_player_name": None,
                        "qb_hit_1_player_id": None,
                        "qb_hit_1_player_name": None,
                        "qb_hit_2_player_id": None,
                        "qb_hit_2_player_name": None,
                        "forced_fumble_player_1_team": None,
                        "forced_fumble_player_1_player_id": None,
                        "forced_fumble_player_1_player_name": None,
                        "forced_fumble_player_2_team": None,
                        "forced_fumble_player_2_player_id": None,
                        "forced_fumble_player_2_player_name": None,
                        "solo_tackle_1_team": None,
                        "solo_tackle_2_team": None,
                        "solo_tackle_1_player_id": None,
                        "solo_tackle_2_player_id": None,
                        "solo_tackle_1_player_name": None,
                        "solo_tackle_2_player_name": None,
                        "assist_tackle_1_player_id": None,
                        "assist_tackle_1_player_name": None,
                        "assist_tackle_1_team": None,
                        "assist_tackle_2_player_id": None,
                        "assist_tackle_2_player_name": None,
                        "assist_tackle_2_team": None,
                        "assist_tackle_3_player_id": None,
                        "assist_tackle_3_player_name": None,
                        "assist_tackle_3_team": None,
                        "assist_tackle_4_player_id": None,
                        "assist_tackle_4_player_name": None,
                        "assist_tackle_4_team": None,
                        "tackle_with_assist": None,
                        "tackle_with_assist_1_player_id": None,
                        "tackle_with_assist_1_player_name": None,
                        "tackle_with_assist_1_team": None,
                        "tackle_with_assist_2_player_id": None,
                        "tackle_with_assist_2_player_name": None,
                        "tackle_with_assist_2_team": None,
                        "pass_defense_1_player_id": None,
                        "pass_defense_1_player_name": None,
                        "pass_defense_2_player_id": None,
                        "pass_defense_2_player_name": None,
                        "fumbled_1_team": None,
                        "fumbled_1_player_id": None,
                        "fumbled_1_player_name": None,
                        "fumbled_2_player_id": None,
                        "fumbled_2_player_name": None,
                        "fumbled_2_team": None,
                        "fumble_recovery_1_team": None,
                        "fumble_recovery_1_yards": None,
                        "fumble_recovery_1_player_id": None,
                        "fumble_recovery_1_player_name": None,
                        "fumble_recovery_2_team": None,
                        "fumble_recovery_2_yards": None,
                        "fumble_recovery_2_player_id": None,
                        "fumble_recovery_2_player_name": None,
                        "sack_player_id": None,
                        "sack_player_name": None,
                        "half_sack_1_player_id": None,
                        "half_sack_1_player_name": None,
                        "half_sack_2_player_id": None,
                        "half_sack_2_player_name": None,
                        "return_team": None,
                        "return_yards": None,
                        "penalty_team": None,
                        "penalty_player_id": None,
                        "penalty_player_name": None,
                        "penalty_yards": None,
                        "replay_or_challenge": None,
                        "replay_or_challenge_result": None,
                        "penalty_type": None,
                        "is_defensive_two_point_attempt": False,
                        "is_defensive_two_point_conv": False,
                        "is_defensive_extra_point_attempt": False,
                        "is_defensive_extra_point_conv": False,
                        "safety_player_name": None,
                        "safety_player_id": None,
                        "series": None,
                        "series_success": None,
                        "series_result": None,
                        "start_time": game_datetime.strftime("%H:%M:%S"),
                        "time_of_day": None,
                        "stadium": None,
                        "weather": None,
                        "special_teams_play": False,
                        "st_play_type": None,
                        "end_yard_line": None,
                        "game_stadium": None,
                        "aborted_play": False,
                        "success": False,
                        "is_out_of_bounds": False,
                    },
                    index=[0],
                )

                if yardline_100 == yds_to_go:
                    # This is auto-set to `0`,
                    # unless it's an actual goal to go situation.
                    temp_df["is_goal_to_go"] = True

                # PBP clean up, replay official overturns play
                if (
                    "the replay official reviewed" in play_desc.lower() and
                    "and the play was overturned." in play_desc.lower()
                ):
                    play_desc = play_desc.split(
                        "and the play was overturned."
                    )[-1]

                # Handler for aborted plays (fumbled snap)
                if "fumbles (aborted)." in play_desc.lower():
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+ \(aborted\)\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    play_desc = re.sub(
                        r"([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+ \(aborted\)\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        "",
                        play_desc
                    )

                    temp_df["fumbled_2_team"] = posteam
                    temp_df["fumbled_2_player_name"] = play_arr[0][0]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][1]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][2]
                    temp_df["fumble_recovery_1_yards"] = 0

                # 4th and 12 onside play
                if "alternative kickoff" in play_desc.lower():
                    play_desc = play_desc.replace(
                        "Alternative kickoff", ""
                    )
                    play_desc = play_desc.replace(
                        "Alternative Kickoff", ""
                    )
                    play_desc = play_desc.replace(
                        "alternative kickoff", ""
                    )
                    temp_df["is_4th_and_12_onside_play"] = True

                # Time management
                if ("end quarter" in play_desc.lower()):
                    temp_df["is_quarter_end"] = True
                elif ("end game" in play_desc.lower()):
                    temp_df["is_quarter_end"] = True
                elif ("two minute warning" in play_desc.lower()):
                    pass
                elif ("timeout #" in play_desc.lower()):
                    temp_df["is_timeout"] = True

                    play_arr = re.findall(
                        r"Timeout #([1-3]) by ([a-zA-Z]+)\.",
                        play_desc
                    )
                    temp_df["timeout_team"] = play_arr[0][1]
                    if play_arr[0][1] == away_team_abv:
                        away_timeouts_remaining -= 1
                    elif play_arr[0][1] == home_team_abv:
                        home_timeouts_remaining -= 1
                    else:
                        temp_team = play_arr[0][1]
                        raise ValueError(
                            f"Unhandled team abbreviation {temp_team}"
                        )
                    if play_arr[0][1] == posteam:
                        posteam_timeouts_remaining
                    elif play_arr[0][1] == defteam:
                        defteam_timeouts_remaining
                    else:
                        temp_team = play_arr[0][1]
                        raise ValueError(
                            f"Unhandled team abbreviation {temp_team}"
                        )
                elif ("tv timeout" in play_desc.lower()):
                    pass
                # Conversions
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "pass" in play_desc.lower() and
                    "catch made" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) steps back to pass\. Catch made by ([a-zA-Z\'\.\-\, ]+) for yards\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["passer_player_name"] = play_arr[0][1]
                    temp_df["receiver_player_name"] = play_arr[0][2]
                    success_or_failure = play_arr[0][6].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "pass" in play_desc.lower() and
                    "intercept" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_defensive_two_point_attempt"] = True
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) steps back to pass\. ([a-zA-Z\'\.\-\, ]+) intercepts the ball\. Pushed out of bounds by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\. [DEFENSIVE|defensive]+ [CONVERSION|conversion]+ [RECOVERY|recovery]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["passer_player_name"] = play_arr[0][1]
                    temp_df["interception_player_name"] = play_arr[0][2]
                    success_or_failure = play_arr[0][6].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True

                    if "suc" in play_arr[0][7]:
                        temp_df["is_defensive_two_point_attempt"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "pass" in play_desc.lower() and
                    "catch made" in play_desc.lower() and
                    "for yards" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) steps back to pass\. Catch made by ([a-zA-Z\'\.\-\, ]+) for yards\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["passer_player_name"] = play_arr[0][1]
                    temp_df["receiver_player_name"] = play_arr[0][2]
                    success_or_failure = play_arr[0][4].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "rushed up the middle" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) rushed up the middle to ([A-Za-z0-9\s]+) for yard[s]?\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["rusher_player_name"] = play_arr[0][1]
                    # temp_df["run_location"] = play_arr[0][1]
                    # temp_df["run_gap"] = play_arr[0][2]

                    success_or_failure = play_arr[0][6].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "rushed" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) to ([A-Za-z0-9\s]+) for yard[s]?\. Tackled by ([a-zA-Z\;\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["rusher_player_name"] = play_arr[0][1]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]

                    success_or_failure = play_arr[0][8].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "rushed up the middle" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) rushed up the middle to ([A-Za-z0-9\s]+) for yard[s]?\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["rusher_player_name"] = play_arr[0][1]
                    temp_df["run_location"] = "middle"
                    # temp_df["run_gap"] = play_arr[0][2]

                    success_or_failure = play_arr[0][4].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                elif (
                    "-point conversion attempt" in play_desc.lower() and
                    "rushed" in play_desc.lower()
                ):
                    play_arr = re.findall(
                        r"([A-Za-z ]+)\-? ?[POINT|point]+ [CONVERSION|conversion]+ [ATTEMPT|attempt]+\. ([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) to ([A-Za-z0-9\s]+) for yard[s]?\. ([A-Za-z ]+)\-? ?[POINT|point]+ [ATTEMPT|attempt]+ ([a-zA-Z]+)\.",
                        play_desc
                    )
                    if "one" in play_arr[0][0].lower():
                        temp_df["is_one_point_attempt"] = True
                    elif "two" in play_arr[0][0].lower():
                        temp_df["is_two_point_attempt"] = True
                    elif "three" in play_arr[0][0].lower():
                        temp_df["is_three_point_attempt"] = True

                    temp_df["rusher_player_name"] = play_arr[0][1]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]

                    success_or_failure = play_arr[0][6].lower()

                    if (
                        "suc" in success_or_failure and
                        "one" in play_arr[0][0].lower()
                    ):
                        temp_df["is_one_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "two" in play_arr[0][0].lower()
                    ):
                        temp_df["is_two_point_attempt_success"] = True
                    elif (
                        "suc" in success_or_failure and
                        "three" in play_arr[0][0].lower()
                    ):
                        temp_df["is_three_point_attempt_success"] = True
                # Pass Plays
                elif (
                    "steps back to pass" not in play_desc.lower() and
                    "incomplete" in play_desc.lower() and
                    "intended for" in play_desc.lower()
                ):
                    # If we're here,
                    # its because we're handling an aborted snap,
                    # that was thrown away (presumably)
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True

                    temp_df["passer_player_name"] = temp_df["fumbled_2_player_name"]

                    play_arr = re.findall(
                        r"[PASS|pass]+ incomplete ([A-Za-z]+) ([A-Za-z]+) intended for ([a-zA-Z\'\.\-\, ]+)",
                        play_desc
                    )
                    temp_df["pass_length"] = play_arr[0][0]
                    temp_df["pass_location"] = play_arr[0][1]
                    temp_df["receiver_player_name"] = play_arr[0][2]
                elif (
                    "steps back to pass" in play_desc.lower() and
                    "incomplete" in play_desc.lower() and
                    "intended for." in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) steps back to pass\. Pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) intended for\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                elif (
                    "steps back to pass" in play_desc.lower() and
                    "incomplete" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    play_desc = play_desc.replace("[","")
                    play_desc = play_desc.replace("]","")
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) steps back to pass\. Pass incomplete ([a-zA-Z]+) ([a-zA-Z]+) intended for ([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "touchdown" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_pass_touchdown"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for " +
                        r"([0-9\-]+) yard[s]?\. TOUCHDOWN\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])
                elif (
                    "pass" in play_desc.lower() and
                    "catch made by" in play_desc.lower() and
                    play_desc.lower().count("fumbles.") == 2 and
                    "forced" not in play_desc.lower() and
                    "lateral" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    # Full play for this madness:
                    #
                    # 'E.Perry pass short right complete.
                    # Catch made by J.Adams for 5 yards.
                    # Lateral to K.Lassiter to MEM 31 for -4 yards.
                    # K.Lassiter FUMBLES.
                    # Fumble RECOVERED by MEM-J.Kibodi at MEM 26.
                    # J.Kibodi FUMBLES.
                    # Fumble RECOVERED by DC-A.Mintze at MEM 24.
                    # Tackled by N.Henderson at MEM 24.'
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True
                    temp_df["is_lateral_reception"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. Lateral to ([a-zA-Z\'\.\-\, ]+) to ([A-Za-z0-9\s]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])
                    temp_df["lateral_receiver_player_name"] = play_arr[0][5]
                    temp_df["lateral_receiving_yards"] = int(play_arr[0][7])
                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][8]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][9]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][9]

                    temp_yl_1 = play_arr[0][11]
                    temp_yl_2 = play_arr[0][15]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2

                    temp_df["fumbled_2_team"] = posteam
                    temp_df["fumbled_2_player_name"] = play_arr[0][12]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][13]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][14]

                    tacklers_arr = play_arr[0][16]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "fumble" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "recovered by" in play_desc.lower() and
                    "tackled by at" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]

                    temp_yl_1 = play_arr[0][9]
                    temp_yl_2 = play_arr[0][10]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_2 - temp_yl_1

                    # tacklers_arr = play_arr[0][10]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "fumble" in play_desc.lower() and
                    "forced by." in play_desc.lower() and
                    "recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    # temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][6]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

                    temp_yl_1 = play_arr[0][8]
                    temp_yl_2 = play_arr[0][10]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_2 - temp_yl_1

                    tacklers_arr = play_arr[0][9]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "fumble" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "recovered by" in play_desc.lower() and
                    "tackled by" not in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]

                    temp_df["fumble_recovery_1_yards"] = 0

                    # tacklers_arr = play_arr[0][10]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "fumble" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([a-zA-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]

                    temp_yl_1 = play_arr[0][9]
                    temp_yl_2 = play_arr[0][11]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_2 - temp_yl_1

                    tacklers_arr = play_arr[0][10]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "ran out of bounds" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for yard[s]?\. ([a-zA-Z\'\.\-\, ]+) ran out of bounds\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    # temp_df["receiving_yards"] = int(play_arr[0][4])
                    # temp_df["passing_yards"] = int(play_arr[0][4])
                    # temp_df["yards_gained"] = int(play_arr[0][4])
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "ran out of bounds" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) ran out of bounds\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "lateral to" in play_desc.lower() and
                    "fumbles." in play_desc.lower() and
                    " out of bounds." in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True
                    temp_df["is_lateral_reception"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. Lateral to ([a-zA-Z\'\.\-\, ]+) to ([A-Za-z0-9\s]+) for ([\-0-9]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\. Out of bounds\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])
                    temp_df["lateral_receiver_player_name"] = play_arr[0][5]
                    temp_df["lateral_receiving_yards"] = int(play_arr[0][7])
                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][8]

                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for yard[s]?\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    # temp_df["receiving_yards"] = int(play_arr[0][4])
                    # temp_df["passing_yards"] = int(play_arr[0][4])
                    # temp_df["yards_gained"] = int(play_arr[0][4])

                    tacklers_arr = play_arr[0][4]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for " +
                        r"([0-9\-]+) yard[s]?\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    tacklers_arr = play_arr[0][5]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "out of bounds." in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_out_of_bounds"] = False
                    temp_df["is_fumble"] = False
                    temp_df["is_fumbled_forced"] = False
                    temp_df["is_fumbled_out_of_bounds"] = False
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for ([0-9\-]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) FUMBLES\, forced by ([a-zA-Z\'\.\-\, ]+)\. Out of bounds\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]
                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_out_of_bounds"] = False
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for yard[s]?\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    # temp_df["receiving_yards"] = int(play_arr[0][4])
                    # temp_df["passing_yards"] = int(play_arr[0][4])
                    # temp_df["yards_gained"] = int(play_arr[0][4])

                    tacklers_arr = play_arr[0][4]
                elif (
                    "pass" in play_desc.lower() and
                    "complete" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_complete_pass"] = True
                    temp_df["is_out_of_bounds"] = False
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"complete[\[\] a-zA-Z\'\.\-\,\s]*\. Catch made by ([a-zA-Z\'\.\-\, ]+) for " +
                        r"([0-9\-]+) yard[s]?\. " +
                        r"Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]
                    temp_df["receiver_player_name"] = play_arr[0][3]
                    temp_df["receiving_yards"] = int(play_arr[0][4])
                    temp_df["passing_yards"] = int(play_arr[0][4])
                    temp_df["yards_gained"] = int(play_arr[0][4])

                    tacklers_arr = play_arr[0][5]
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "ran out of bounds." in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) [INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)[\[\] a-zA-Z\'\.\-\,\s]*\. Intercepted by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) ran out of bounds\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    # tacklers_arr = play_arr[0][6]
                    # temp_yl_1 = play_arr[0][5]
                    # temp_yl_2 = play_arr[0][7]

                    # temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    # temp_yl_2 = get_yardline(temp_yl_2, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = 0
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"[INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)\. " +
                        r"Intercepted by ([a-zA-Z\'\.\-\, ]+) at " +
                        r"([A-Za-z0-9\s]+)\. " +
                        r"Pushed out of bounds by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][6]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][7]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = temp_yl_2 - temp_yl_1
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "touchdown" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    temp_df["is_return_touchdown"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) [INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)[\[\] a-zA-Z\'\.\-\,\s]*\. Intercepted by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. [TOUCHDOWN|touchdown]+\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    temp_yl_1 = play_arr[0][5]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = 100 - temp_yl_1
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "lateral to" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    temp_df["is_lateral_return"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) [INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)[\[\] a-zA-Z\'\.\-\,\s]*\. Intercepted by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Lateral to ([a-zA-Z\'\.\-\, ]+) to ([A-Za-z0-9\s]+) for ([\-0-9]+) yard[s]?\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    temp_df["lateral_interception_player_name"] = play_arr[0][6]

                    tacklers_arr = play_arr[0][9]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][10]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = temp_yl_2 - temp_yl_1
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "lateral to" in play_desc.lower() and
                    "recovered by" not in play_desc.lower() and
                    "out of bounds" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    temp_df["is_lateral_return"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) [INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)[\[\] a-zA-Z\'\.\-\,\s]*\. Intercepted by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Lateral to ([a-zA-Z\'\.\-\, ]+) to ([A-Za-z0-9\s]+) for ([\-0-9]+) yard[s]?\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    temp_df["lateral_interception_player_name"] = play_arr[0][6]

                    tacklers_arr = play_arr[0][9]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][10]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = temp_yl_2 - temp_yl_1
                elif (
                    "pass" in play_desc.lower() and
                    "intercepted" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_interception"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) pass ([a-zA-Z]+) ([a-zA-Z]+) [INTERCEPTED|intercepted]+ at ([A-Za-z0-9\s]+)[\[\] a-zA-Z\'\.\-\,\s]*\. Intercepted by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["pass_length"] = play_arr[0][1]
                    temp_df["pass_location"] = play_arr[0][2]

                    temp_df["interception_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][6]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][7]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)

                    temp_df["return_team"] = defteam
                    temp_df["return_yards"] = temp_yl_2 - temp_yl_1
                elif ("spikes the ball" in play_desc.lower()):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_incomplete_pass"] = True
                    temp_df["is_qb_spike"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) spikes the ball\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0]
                # Pass (sacks)
                elif (
                    "pass" in play_desc.lower() and
                    "sacked at" in play_desc.lower() and
                    "for yards" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_sack"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) steps back to pass\. Sacked at ([A-Za-z0-9\s]+) for yard[s]? \(([a-zA-Z\'\.\;\-\, ]+)\)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    # temp_df["yards_gained"] = int(play_arr[0][2])
                    tacklers_arr = play_arr[0][2]
                    sack_players_arr = play_arr[0][2]
                elif (
                    "pass" in play_desc.lower() and
                    "sacked at" in play_desc.lower()
                ):
                    temp_df["is_pass_attempt"] = True
                    temp_df["is_sack"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) steps back to pass\. Sacked at ([A-Za-z0-9\s]+) for ([\-0-9]+) yard[s]? \(([a-zA-Z\'\.\;\-\, ]+)\)\.",
                        play_desc
                    )
                    temp_df["passer_player_name"] = play_arr[0][0]
                    temp_df["yards_gained"] = int(play_arr[0][2])
                    tacklers_arr = play_arr[0][3]
                    sack_players_arr = play_arr[0][3]
                # Run plays
                elif (
                    "rushed up the middle for" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" not in play_desc.lower() and
                    "pushed out of bounds by" not in play_desc.lower()
                ):
                    temp_df["run_location"] = "middle"
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed up the middle for ([\-0-9]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][2]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][3]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][4]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][5]
                elif (
                    "rushed" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["run_location"] = "middle"
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yard[s]?\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][6]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

                    tacklers_arr = play_arr[0][9]
                elif (
                    "rushed up the middle for yards" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["run_location"] = "middle"
                    temp_df["is_rush_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed up the middle " +
                        r"for yard[s]?\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    # temp_df["rushing_yards"] = int(play_arr[0][1])
                    # temp_df["yards_gained"] = int(play_arr[0][1])
                    tacklers_arr = play_arr[0][1]
                elif (
                    "rushed up the middle for" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["run_location"] = "middle"
                    temp_df["is_rush_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed up the middle " +
                        r"for ([\-0-9]+) yard[s]?\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])
                    tacklers_arr = play_arr[0][2]
                elif (
                    "rushed" in play_desc.lower() and
                    "tackled by" in play_desc.lower() and
                    "for yards" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"for yard[s]?\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    # temp_df["rushing_yards"] = int(play_arr[0][3])
                    # temp_df["yards_gained"] = int(play_arr[0][3])
                    tacklers_arr = play_arr[0][3]
                elif (
                    "rushed" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) " +
                        r"for ([\-0-9]+) yard[s]?\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                    tacklers_arr = play_arr[0][4]
                elif (
                    "rushed" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) for yard[s]?\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    # temp_df["rushing_yards"] = int(play_arr[0][3])
                    # temp_df["yards_gained"] = int(play_arr[0][3])
                    tacklers_arr = play_arr[0][3]
                elif (
                    "rushed up the middle" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed up the middle for ([\-0-9]+) yard[s]?\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = "middle"
                    # temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])
                    tacklers_arr = play_arr[0][2]
                elif (
                    "rushed" in play_desc.lower() and
                    "lateral to " in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_lateral_rush"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yard[s]?\. Lateral to ([a-zA-Z\'\.\-\, ]+) to ([A-Za-z0-9\s]+) for ([0-9\-]) yard[s]?\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                    temp_df["lateral_rusher_player_name"] = play_arr[0][4]
                    temp_df["lateral_rushing_yards"] = int(play_arr[0][6])
                    temp_df["yards_gained"] += int(play_arr[0][6])

                    tacklers_arr = play_arr[0][7]

                elif (
                    "rushed" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yard[s]?\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                    tacklers_arr = play_arr[0][4]
                elif (
                    "rushed" in play_desc.lower() and
                    "up the middle" in play_desc.lower() and
                    "touchdown" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_rush_touchdown"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed " +
                        r"up the middle for " +
                        r"([\-0-9]+) yard[s]?\. [TOUCHDOWN|touchdown]+",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])
                elif (
                    "rushed" in play_desc.lower() and
                    "touchdown" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_rush_touchdown"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) rushed " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) for " +
                        r"([\-0-9]+) yard[s]?\. [TOUCHDOWN|touchdown]+",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                # Runs (scrambles)
                elif (
                    "scrambles" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) for yards\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    tacklers_arr = play_arr[0][3]
                    temp_yl_1 = play_arr[0][4]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_df["rushing_yards"] = yardline_100 - temp_yl_1
                    temp_df["yards_gained"] = yardline_100 - temp_yl_1
                elif (
                    "scrambles" in play_desc.lower() and
                    "up the middle" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"up the middle for ([\-0-9]+) yards\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])
                    temp_df["run_location"] = "middle"

                    tacklers_arr = play_arr[0][2]
                elif (
                    "scrambles" in play_desc.lower() and
                    "for yards" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) for yards\. " +
                        r"Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    # temp_df["rushing_yards"] = int(play_arr[0][3])
                    # temp_df["yards_gained"] = int(play_arr[0][3])

                    tacklers_arr = play_arr[0][3]
                elif (
                    "scrambles up the middle" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"up the middle for ([\-0-9]+) yards\. " +
                        r"Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = "middle"
                    # temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])

                    tacklers_arr = play_arr[0][2]
                elif (
                    "scrambles" in play_desc.lower() and
                    "touchdown" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    temp_df["is_touchdown"] = True
                    temp_df["is_rush_touchdown"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yards\. [TOUCHDOWN|touchdown]+\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                elif (
                    "scrambles" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yards\. " +
                        r"Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])

                    tacklers_arr = play_arr[0][4]
                elif (
                    "scrambles" in play_desc.lower() and
                    "tackled by at" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles " +
                        r"([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yards\. " +
                        r"Tackled by at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])

                    # tacklers_arr = play_arr[0][4]
                elif (
                    "scrambles" in play_desc.lower() and
                    "tackled by" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yards\. ([a-zA-Z\'\.\-\, ]+) [FUMBLES|fumbles]+\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([A-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][5]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][6]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

                    tacklers_arr = play_arr[0][9]
                elif (
                    "scrambles up the middle" in play_desc.lower() and
                    "ran out of bounds" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles up the middle for ([\-0-9]+) yards\. ([a-zA-Z\'\.\-\, ]+) ran out of bounds\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = "middle"
                    # temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][1])
                    temp_df["yards_gained"] = int(play_arr[0][1])
                elif (
                    "scrambles" in play_desc.lower() and
                    "ran out of bounds" in play_desc.lower()
                ):
                    temp_df["is_rush_attempt"] = True
                    temp_df["is_qb_scramble"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) scrambles ([a-zA-Z]+) ([a-zA-Z]+) for ([\-0-9]+) yards\. ([a-zA-Z\'\.\-\, ]+) ran out of bounds\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_df["run_location"] = play_arr[0][1]
                    temp_df["run_gap"] = play_arr[0][2]
                    temp_df["rushing_yards"] = int(play_arr[0][3])
                    temp_df["yards_gained"] = int(play_arr[0][3])
                # Runs (kneeldowns)
                elif ("kneels at the" in play_desc.lower()):
                    temp_df["is_qb_kneel"] = True
                    temp_df["is_rush_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kneels at the ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["rusher_player_name"] = play_arr[0][0]
                    temp_yl_1 = play_arr[0][1]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_df["rushing_yards"] = yardline_100 - temp_yl_1
                    temp_df["yards_gained"] = yardline_100 - temp_yl_1
                    del temp_yl_1
                # Field Goals
                elif (
                    "field goal attempt is" in play_desc.lower() and
                    "penalty on" not in play_desc.lower() and
                    "no play" not in play_desc.lower()
                ):
                    temp_df["is_field_goal_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) ([0-9]+) yard[s]? field goal attempt is ([a-zA-Z\s]+)\, [Center|center]+\-([a-zA-Z\'\.\-\, ]+), [Holder|holder]+\-([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["field_goal_result"] = play_arr[0][2]
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["holder_player_name"] = play_arr[0][4]
                elif (
                    "field goal attempt is" in play_desc.lower() and
                    "penalty on" in play_desc.lower() and
                    "no play" in play_desc.lower()
                ):
                    temp_df["is_field_goal_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) yard[s]? field goal attempt is ([a-zA-Z\s]+)\, [Center|center]+\-([a-zA-Z\'\.\-\, ]+), [Holder|holder]+\-([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    if len(play_arr) == 0:
                        play_arr = re.findall(
                            r"([a-zA-Z\'\.\-\, ]+) ([0-9]+) yard[s]? field goal attempt is ([a-zA-Z\s]+)\, [Center|center]+\-([a-zA-Z\'\.\-\, ]+), [Holder|holder]+\-([a-zA-Z\'\.\-\, ]+)\.",
                            play_desc
                        )
                        temp_df["kicker_player_name"] = play_arr[0][0]
                        temp_df["kick_distance"] = int(play_arr[0][1])
                        temp_df["field_goal_result"] = play_arr[0][2]
                        temp_df["long_snapper_player_name"] = play_arr[0][3]
                        temp_df["holder_player_name"] = play_arr[0][4]
                    else:
                        temp_df["kicker_player_name"] = play_arr[0][0]
                        # temp_df["kick_distance"] = int(play_arr[0][1])
                        temp_df["field_goal_result"] = play_arr[0][1]
                        temp_df["long_snapper_player_name"] = play_arr[0][2]
                        temp_df["holder_player_name"] = play_arr[0][3]
                # Kickoff
                elif (
                    "kicks yards" in play_desc.lower() and
                    "muffs catch" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) [MUFFS|muffs]+ catch\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    # temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][3]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][3]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][4]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][5]

                    temp_yl_1 = play_arr[0][6]
                    temp_yl_2 = play_arr[0][8]

                    tacklers_arr = play_arr[0][7]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    # test = temp_yl_1 - temp_yl_2
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "returns the kickoff" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_out_of_bounds"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from " +
                        r"([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. " +
                        r"([a-zA-Z\'\.\-\, ]+) returns the kickoff\. " +
                        r"Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_yl_1 = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][5]
                    temp_yl_2 = play_arr[0][6]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["return_yards"] = temp_yl_1 - temp_yl_2

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "muffs catch" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) [MUFFS|muffs]+ catch\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][5]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]

                    temp_yl_1 = play_arr[0][7]
                    temp_yl_2 = play_arr[0][9]

                    tacklers_arr = play_arr[0][8]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    # test = temp_yl_1 - temp_yl_2
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "muffs catch" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) [MUFFS|muffs]+ catch\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][5]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]

                    temp_yl_1 = play_arr[0][7]
                    temp_yl_2 = play_arr[0][9]

                    tacklers_arr = play_arr[0][8]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    # test = temp_yl_1 - temp_yl_2
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "returns the kickoff" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) returns the kickoff\. ([a-zA-Z\'\.\-\, ]+) FUMBLES, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble RECOVERED by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]

                    temp_yl_1 = play_arr[0][9]
                    temp_yl_2 = play_arr[0][11]

                    tacklers_arr = play_arr[0][10]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_2 - temp_yl_1
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "returns the kickoff" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) returns the kickoff\. ([a-zA-Z\'\.\-\, ]+) FUMBLES, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble RECOVERED by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    temp_df["forced_fumble_player_1_team"] = defteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][8]

                    # temp_yl_1 = play_arr[0][9]
                    temp_yl_2 = play_arr[0][9]

                    # tacklers_arr = play_arr[0][10]

                    # temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = 0
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "returns the kickoff" in play_desc.lower() and
                    "fumbles" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" not in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) returns the kickoff\. ([a-zA-Z\'\.\-\, ]+) FUMBLES\. Fumble RECOVERED by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )

                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]

                    temp_df["fumbled_1_team"] = posteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][5]

                    # temp_df["forced_fumble_player_1_team"] = defteam
                    # temp_df["forced_fumble_player_1_player_name"] = play_arr[0][6]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][6]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][7]

                    # temp_yl_1 = play_arr[0][9]
                    temp_yl_2 = play_arr[0][8]

                    # tacklers_arr = play_arr[0][10]

                    # temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = 0
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "returns the kickoff" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from " +
                        r"([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. " +
                        r"([a-zA-Z\'\.\-\, ]+) returns the kickoff\. " +
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_yl_1 = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][5]
                    temp_yl_2 = play_arr[0][6]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["return_yards"] = temp_yl_1 - temp_yl_2

                    if temp_yl_2 > 80:
                        temp_df["is_kickoff_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "kicks" in play_desc.lower() and
                    "touchback" in play_desc.lower()
                ):
                    temp_df["is_kickoff_attempt"] = True
                    temp_df["is_touchback"] = True
                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) kicks ([\-0-9]+) yard[s]? from ([A-Za-z0-9\s]+) to the ([A-Za-z0-9\s]+)\. Touchback\.",
                        play_desc
                    )
                    temp_df["kicker_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_yl_1 = play_arr[0][3]
                # Punts
                elif (
                    "punt" in play_desc.lower() and
                    "returned punt from" in play_desc.lower() and
                    "fumbles, forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by at" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) returned punt from the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) FUMBLES\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    temp_df["fumbled_1_team"] = defteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][6]
                    temp_df["forced_fumble_player_1_team"] = posteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][8]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][9]

                    temp_yl_1 = play_arr[0][10]
                    temp_yl_2 = play_arr[0][11]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "punt" in play_desc.lower() and
                    "returned punt from" in play_desc.lower() and
                    "fumbles, forced by" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) returned punt from the ([A-Za-z0-9\s]+)\. ([a-zA-Z\'\.\-\, ]+) FUMBLES\, forced by ([a-zA-Z\'\.\-\, ]+)\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    temp_df["fumbled_1_team"] = defteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][6]
                    temp_df["forced_fumble_player_1_team"] = posteam
                    temp_df["forced_fumble_player_1_player_name"] = play_arr[0][7]
                    temp_df["fumble_recovery_1_team"] = play_arr[0][8]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][9]

                    # temp_yl_1 = play_arr[0][10]
                    # temp_yl_2 = play_arr[0][11]

                    # temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    # temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    # temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["fumble_recovery_1_yards"] = 0
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "punts yards" in play_desc.lower() and
                    "returned punt from" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) returned punt from the ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    # temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][2]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][3]
                    tacklers_arr = play_arr[0][5]
                    # temp_yl_1 = play_arr[0][5]
                    # temp_yl_2 = play_arr[0][7]

                    # temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    # temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    # temp_df["return_yards"] = temp_yl_1 - temp_yl_2

                    # if temp_yl_2 > 80:
                    #     temp_df["is_punt_inside_twenty"] = True
                    # del temp_yl_1, temp_yl_2
                elif (
                    "punt" in play_desc.lower() and
                    "returned punt from" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) returned punt from the ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][6]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][7]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["return_yards"] = temp_yl_1 - temp_yl_2

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "punts" in play_desc.lower() and
                    "muffs catch" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by at" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([0-9]+) yards to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) [MUFFS|muffs]+ catch\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    temp_df["fumbled_1_team"] = defteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][5]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
                    # tacklers_arr = play_arr[0][8]
                    temp_yl_1 = play_arr[0][7]
                    temp_yl_2 = play_arr[0][8]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "punts" in play_desc.lower() and
                    "muffs catch" in play_desc.lower() and
                    "fumble recovered by" in play_desc.lower() and
                    "tackled by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_fumble"] = True
                    temp_df["is_fumble_not_forced"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([0-9]+) yards to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) [MUFFS|muffs]+ catch\. Fumble [RECOVERED|recovered]+ by ([A-Za-z]+)\-? ?([a-zA-Z\'\.\-\, ]+) at ([A-Za-z0-9\s]+)\. Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    temp_df["fumbled_1_team"] = defteam
                    temp_df["fumbled_1_player_name"] = play_arr[0][4]

                    temp_df["fumble_recovery_1_team"] = play_arr[0][5]
                    temp_df["fumble_recovery_1_player_name"] = play_arr[0][6]
                    tacklers_arr = play_arr[0][8]
                    temp_yl_1 = play_arr[0][7]
                    temp_yl_2 = play_arr[0][9]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["fumble_recovery_1_yards"] = temp_yl_1 - temp_yl_2
                    temp_df["return_yards"] = 0

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "punt" in play_desc.lower() and
                    "returned punt from" not in play_desc.lower() and
                    "out of bounds" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_punt_out_of_bounds"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. Out of bounds\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]

                    temp_yl_1 = play_arr[0][2]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    if temp_yl_1 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                elif (
                    "punt" in play_desc.lower() and
                    "touchback" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_touchback"] = True
                    temp_df["is_punt_in_endzone"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z]+) [End|end]+ [Zone|zone]+, [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. Touchback\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                elif (
                    "punts yards" in play_desc.lower() and
                    "fair catch by" in play_desc.lower() and
                    "no play" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_punt_downed"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. Fair catch by ([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    # temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][2]
                    temp_df["punt_returner_player_name"] = play_arr[0][3]

                    temp_yl_1 = play_arr[0][1]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    if temp_yl_1 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                elif (
                    "punt" in play_desc.lower() and
                    "fair catch by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_punt_downed"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. Fair catch by ([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["punt_returner_player_name"] = play_arr[0][4]

                    temp_yl_1 = play_arr[0][2]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    if temp_yl_1 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                elif (
                    "punt" in play_desc.lower() and
                    "downed by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_punt_downed"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. Downed by ([a-zA-Z\'\.\-\, ]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]

                    temp_yl_1 = play_arr[0][2]
                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    if temp_yl_1 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                elif (
                    "punt" in play_desc.lower() and
                    "returned punt from" in play_desc.lower() and
                    "pushed out of bounds by" in play_desc.lower()
                ):
                    temp_df["is_punt_attempt"] = True
                    temp_df["is_out_of_bounds"] = True

                    play_arr = re.findall(
                        r"([a-zA-Z\'\.\-\, ]+) punts ([\-0-9]+) yard[s]? to ([A-Za-z0-9\s]+), [center|Center]+\-? ?([a-zA-Z\'\.\-\, ]+)\. ([a-zA-Z\'\.\-\, ]+) returned punt from the ([A-Za-z0-9\s]+)\. Pushed out of bounds by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    temp_df["punter_player_name"] = play_arr[0][0]
                    temp_df["kick_distance"] = int(play_arr[0][1])
                    temp_df["long_snapper_player_name"] = play_arr[0][3]
                    temp_df["kickoff_returner_player_name"] = play_arr[0][4]
                    tacklers_arr = play_arr[0][6]
                    temp_yl_1 = play_arr[0][5]
                    temp_yl_2 = play_arr[0][7]

                    temp_yl_1 = get_yardline(temp_yl_1, posteam)
                    temp_yl_2 = get_yardline(temp_yl_2, posteam)
                    temp_df["return_yards"] = temp_yl_1 - temp_yl_2

                    if temp_yl_2 > 80:
                        temp_df["is_punt_inside_twenty"] = True
                    del temp_yl_1, temp_yl_2
                elif (
                    "tackled by" in play_desc.lower()
                ):
                    # to handle weird situations where this is the only part of the play that still exists.
                    play_arr = re.findall(
                        r"Tackled by ([a-zA-Z\.\-\,\'\;\s]+) at ([A-Za-z0-9\s]+)\.",
                        play_desc
                    )
                    tacklers_arr = play_arr[0][0]
                # Penalties (before the snap)
                elif (
                    "penalty" in play_desc.lower()
                ):
                    # We'll handle penalty data later.
                    pass
                else:
                    raise Exception(
                        f"Unhandled play: {play_desc}"
                    )

                if len(tacklers_arr) > 0 and "," in tacklers_arr:
                    temp_df[[
                        "assist_tackle_1_player_name",
                        "assist_tackle_2_player_name"
                    ]] = tacklers_arr.split(",")
                if len(tacklers_arr) > 0 and ";" in tacklers_arr:
                    temp_df[[
                        "assist_tackle_1_player_name",
                        "assist_tackle_2_player_name"
                    ]] = tacklers_arr.split(";")
                elif len(tacklers_arr) > 0:
                    temp_df["solo_tackle_1_player_name"] = tacklers_arr
                del tacklers_arr

                if len(sack_players_arr) > 0 and "," in sack_players_arr:
                    temp_df[[
                        "half_sack_1_player_name",
                        "half_sack_2_player_name"
                    ]] = sack_players_arr.split(",")
                if len(sack_players_arr) > 0 and ";" in sack_players_arr:
                    temp_df[[
                        "half_sack_1_player_name",
                        "half_sack_2_player_name"
                    ]] = sack_players_arr.split(";")
                elif len(sack_players_arr) > 0:
                    temp_df["sack_player_name"] = sack_players_arr
                del sack_players_arr

                if ("penalty" in play_desc.lower()):
                    temp_df["is_penalty"] = True

                    play_arr = re.findall(
                        r"PENALTY on ([A-Z]+)\-? ?([a-zA-Z\'\.\-\, ]+)\, ([a-zA-Z\s\/]+)\, ([0-9]+) yard[s]?,\.? ([a-zA-Z\s]+)\.",
                        play_desc
                    )
                    temp_df["penalty_team"] = play_arr[0][0]
                    temp_df["penalty_player_name"] = play_arr[0][1]
                    temp_df["penalty_type"] = play_arr[0][2]
                    temp_df["penalty_yards"] = int(play_arr[0][3])

                pbp_df_arr.append(temp_df)
                del temp_df

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    return pbp_df


def get_ufl_pbp(
    season: int,
    save_csv: bool = False,
    save_parquet: bool = False,
    # save_json: bool = True,
) -> pd.DataFrame:
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
    # columns_order = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/125.0.0.0 Safari/537.36",
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

    if len(schedule_df) == 0:
        schedule_df = pd.read_parquet(
            "https://github.com/armstjc/ufl-data-repository/releases/"
            + f"download/ufl-schedule/{season}_ufl_schedule.parquet"
        )
        schedule_df = schedule_df[
            schedule_df["week_num"] == 1
        ]    

    ufl_game_id_arr = schedule_df["ufl_game_id"].to_numpy()
    ufl_game_type_arr = schedule_df["season_type"].to_numpy()
    week_title_arr = schedule_df["week_title"].to_numpy()

    for g in tqdm(range(0, len(ufl_game_id_arr))):
        ufl_game_id = ufl_game_id_arr[g]

        url = (
            "https://api.foxsports.com/bifrost/v1/ufl/event/"
            + f"{ufl_game_id}/data?apikey={fox_key}"
        )
        # play_id = 0

        response = requests.get(url=url, headers=headers)
        game_json = json.loads(response.text)

        season_type = ufl_game_type_arr[g]
        week = int(
            week_title_arr[g].lower().replace(
                "week ", ""
            ).replace(
                "playoffs", "11"
            ).replace(
                "championship", "12"
            )
        )

        plays_df = parser(
            game_json=game_json,
            ufl_game_id=ufl_game_id,
            season=season,
            season_type=season_type,
            week=week
        )

        pbp_df_arr.append(plays_df)

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["last_updated"] = now
    # print(pbp_df)

    if save_csv is True:
        pbp_df.to_csv(
            f"pbp/{season}_ufl_pbp.csv",
            index=False
        )

    if save_parquet is True:
        pbp_df.to_parquet(
            f"pbp/{season}_ufl_pbp.parquet",
            index=False
        )

    return pbp_df


def parse_usfl_pbp():
    path = "usfl_game_logs/*.json"
    json_files_arr = glob(pathname=path)
    now = datetime.now()
    pbp_df_arr = []
    pbp_df = pd.DataFrame()
    # print(json_files_arr)
    for usfl_game in tqdm(json_files_arr):
        f_path = format_folder_path(usfl_game)
        # print(f_path)
        game_id = int(f_path.split('/')[1].split(".")[0])

        with open(f_path, "r") as f:
            json_data = json.loads(f.read())
        season = int(str(json_data["header"]["eventTime"]).split("-")[0])
        week = json_data["metadata"]["parameters"]["canonicalUrl"]
        week = week.replace("/usfl/week-", "")
        week = week.split("-")[0]

        if "playoffs" in str(week):
            week = 11
        if "semifinals" in str(week):
            week = 11
        elif "championship" in str(week):
            week = 12
        else:
            week = int(week)

        temp_df = parser(
            game_json=json_data,
            ufl_game_id=game_id,
            season=season,
            season_type="REGULAR SEASON",
            week=week
        )
        pbp_df_arr.append(temp_df)

        del f_path
        del game_id
        del json_data
        del season
        del temp_df

    pbp_df = pd.concat(pbp_df_arr, ignore_index=True)
    pbp_df["last_updated"] = now
    pbp_df.to_csv("pbp/usfl_pbp.csv", index=False)


def main():
    now = datetime.now()

    arg_parser = ArgumentParser()

    arg_parser.add_argument(
        "--save_csv", default=False, action=BooleanOptionalAction
    )
    arg_parser.add_argument(
        "--save_parquet", default=False, action=BooleanOptionalAction
    )
    arg_parser.add_argument(
        "--save_json", default=False, action=BooleanOptionalAction
    )

    args = arg_parser.parse_args()
    # parse_usfl_pbp()

    if now.month == 3 and now.day >= 28:
        get_ufl_pbp(
            season=now.year,
            save_csv=args.save_csv,
            save_parquet=args.save_parquet
        )
    elif now.month > 3:
        get_ufl_pbp(
            season=now.year,
            save_csv=args.save_csv,
            save_parquet=args.save_parquet
        )
    else:
        get_ufl_pbp(
            season=now.year-1,
            save_csv=args.save_csv,
            save_parquet=args.save_parquet
        )


if __name__ == "__main__":
    # main()
    for i in range(2025, 2024, -1):
        get_ufl_pbp(
            season=i,
            save_csv=True,
            save_parquet=True
        )
