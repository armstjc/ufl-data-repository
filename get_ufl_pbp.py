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


def parser(
    game_json: dict,
    ufl_game_id: int,
    season: int,
    season_type: str,
    week: int,
) -> pd.DataFrame:
    """
    """
    # Static data
    pbp_df = pd.DataFrame()
    pbp_df_arr = []

    temp_df = pd.DataFrame()

    away_team_id = int(
        game_json[
            "header"]["leftTeam"]["entityLink"]["layout"]["tokens"]["id"]
    )
    away_team_abv = game_json["header"]["leftTeam"]["name"]

    home_team_id = int(
        game_json[
            "header"]["rightTeam"]["entityLink"]["layout"]["tokens"]["id"]
    )
    home_team_abv = game_json["header"]["rightTeam"]["name"]

    game_id = f"{season}_{week:02d}_{away_team_abv}_{home_team_abv}"

    stadium = game_json["header"]["venueName"]
    game_datetime = game_json["header"]["eventTime"]

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
            drive_id += 1
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

            for play in drive["plays"]:
                play_id += 1
                score_differential = \
                    posteam_score_post - defteam_score_post
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
                    down, yds_to_go = \
                        down_and_distance_temp.lower().split(" and ")
                    down = int(down)
                    yds_to_go = int(yds_to_go)
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
                time_min, time_sec = time.split(":")
                time_min = int(time_min)
                time_sec = int(time_sec)

                desc = play["playDescription"]

                if yrdln is not None:
                    side_of_field = re.sub(
                        r"([0-9\s]+)",
                        r"",
                        yrdln
                    )
                    yardline_100 = get_yardline(
                        yrdln,
                        posteam
                    )

                    if yardline_100 <= 20:
                        is_drive_inside20 = 1
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
                    half_seconds_remaining = ((2 - play_quarter_num) * 900) + \
                        (time_min * 60) + time_sec
                    game_seconds_remaining = ((4 - play_quarter_num) * 900) + \
                        (time_min * 60) + time_sec
                elif half_num == 2:
                    quarter_seconds_remaining = (time_min * 60) + time_sec
                    half_seconds_remaining = ((4 - play_quarter_num) * 900) + \
                        (time_min * 60) + time_sec
                    game_seconds_remaining = ((4 - play_quarter_num) * 900) + \
                        (time_min * 60) + time_sec
                elif half_num == 3:
                    quarter_seconds_remaining = 0
                    half_seconds_remaining = 0
                    game_seconds_remaining = 0

                if yardline_100 == yds_to_go:
                    # This is auto-set to `0`,
                    # unless it's an actual goal to go situation.
                    is_goal_to_go = 1

                temp_df = pd.DataFrame(
                    {
                        "season": None,
                        "play_id":play_id,
                        "game_id":game_id,
                        "home_team":home_team_abv,
                        "away_team":away_team_abv,
                        "season_type":season_type,
                        "week":week,
                        "posteam":posteam,
                        "posteam_type":posteam_type,
                        "defteam":defteam,
                        "side_of_field":side_of_field,
                        "yardline_100":yardline_100,
                        "game_date":game_datetime,
                        "quarter_seconds_remaining":quarter_seconds_remaining,
                        "half_seconds_remaining":half_seconds_remaining,
                        "game_seconds_remaining":game_seconds_remaining,
                        "game_half":game_half,
                        "quarter_end":0,
                        "drive":drive_id,
                        "scoring_play":False,
                        "qtr":quarter_num,
                        "down":down,
                        "goal_to_go":is_goal_to_go,
                        "time":time,
                        "yrdln":yrdln,
                        "ydstogo":yds_to_go,
                        "ydsnet":0,
                        "desc":desc,
                        "play_type":None,
                        "yards_gained":None,
                        "shotgun":False,
                        "no_huddle":False,
                        "qb_dropback":False,
                        "qb_kneel":False,
                        "qb_spike":False,
                        "qb_scramble":False,
                        "pass_length":None,
                        "pass_location":None,
                        "air_yards":None,
                        "yards_after_catch":None,
                        "run_location":None,
                        "run_gap":None,
                        "field_goal_result":None,
                        "kick_distance":None,
                        "extra_point_result":None,
                        "two_point_conv_result":None,
                        "home_timeouts_remaining":home_timeouts_remaining,
                        "away_timeouts_remaining":away_timeouts_remaining,
                        "is_timeout":False,
                        "timeout_team":None,
                        "td_team":None,
                        "td_player_name":None,
                        "td_player_id":None,
                        "posteam_timeouts_remaining":posteam_timeouts_remaining,
                        "defteam_timeouts_remaining":defteam_timeouts_remaining,
                        "total_home_score":total_home_score,
                        "total_away_score":total_away_score,
                        "posteam_score":posteam_score,
                        "defteam_score":defteam_score,
                        "score_differential":score_differential,
                        "posteam_score_post":posteam_score_post,
                        "defteam_score_post":defteam_score_post,
                        "score_differential_post":defteam_score_post,
                        "is_punt_blocked":False,
                        "is_first_down_rush":False,
                        "is_first_down_pass":False,
                        "is_first_down_penalty":False,
                        "is_third_down_converted":False,
                        "is_third_down_failed":False,
                        "is_fourth_down_converted":False,
                        "is_fourth_down_failed":False,
                        "is_incomplete_pass":False,
                        "is_touchback":False,
                        "is_interception":False,
                        "is_punt_inside_twenty":False,
                        "is_punt_in_endzone":False,
                        "is_punt_out_of_bounds":False,
                        "is_punt_downed":False,
                        "is_punt_fair_catch":False,
                        "is_kickoff_inside_twenty":False,
                        "is_kickoff_in_endzone":False,
                        "is_kickoff_out_of_bounds":False,
                        "is_kickoff_downed":False,
                        "is_kickoff_fair_catch":False,
                        "is_fumble_forced":False,
                        "is_fumble_not_forced":False,
                        "is_fumble_out_of_bounds":False,
                        "is_solo_tackle":False,
                        "is_safety":False,
                        "is_penalty":False,
                        "is_tackled_for_loss":False,
                        "is_fumble_lost":False,
                        "is_own_kickoff_recovery":False,
                        "is_own_kickoff_recovery_td":False,
                        "is_qb_hit":False,
                        "is_rush_attempt":False,
                        "is_pass_attempt":False,
                        "is_sack":False,
                        "is_touchdown":False,
                        "pass_touchdown":False,
                        "rush_touchdown":False,
                        "return_touchdown":False,
                        "extra_point_attempt":False,
                        "two_point_attempt":False,
                        "field_goal_attempt":False,
                        "kickoff_attempt":False,
                        "punt_attempt":False,
                        "fumble":False,
                        "complete_pass":False,
                        "assist_tackle":False,
                        "lateral_reception":False,
                        "lateral_rush":False,
                        "lateral_return":False,
                        "lateral_recovery": False,
                        passer_player_id	passer_player_name	passing_yards	receiver_player_id	receiver_player_name	receiving_yards	rusher_player_id	
                        rusher_player_name	rushing_yards	lateral_receiver_player_id	lateral_receiver_player_name	
                        lateral_receiving_yards	lateral_rusher_player_id	lateral_rusher_player_name	lateral_rushing_yards	lateral_sack_player_id	lateral_sack_player_name	
                        interception_player_id	interception_player_name	lateral_interception_player_id	lateral_interception_player_name	
                        punt_returner_player_id	punt_returner_player_name	lateral_punt_returner_player_id	lateral_punt_returner_player_name	
                        kickoff_returner_player_name	kickoff_returner_player_id	lateral_kickoff_returner_player_id	lateral_kickoff_returner_player_name	
                        punter_player_id	punter_player_name	kicker_player_name	kicker_player_id	own_kickoff_recovery_player_id	own_kickoff_recovery_player_name	
                        blocked_player_id	blocked_player_name	tackle_for_loss_1_player_id	tackle_for_loss_1_player_name	
                        tackle_for_loss_2_player_id	tackle_for_loss_2_player_name	qb_hit_1_player_id	qb_hit_1_player_name	qb_hit_2_player_id	qb_hit_2_player_name	
                        forced_fumble_player_1_team	forced_fumble_player_1_player_id	forced_fumble_player_1_player_name	
                        forced_fumble_player_2_team	forced_fumble_player_2_player_id	forced_fumble_player_2_player_name	solo_tackle_1_team	
                        solo_tackle_2_team	solo_tackle_1_player_id	solo_tackle_2_player_id	solo_tackle_1_player_name	solo_tackle_2_player_name	
                        assist_tackle_1_player_id	assist_tackle_1_player_name	assist_tackle_1_team
                        assist_tackle_2_player_id	assist_tackle_2_player_name	assist_tackle_2_team
                        assist_tackle_3_player_id	assist_tackle_3_player_name	assist_tackle_3_team
                        assist_tackle_4_player_id	assist_tackle_4_player_name	assist_tackle_4_team	tackle_with_assist
                        tackle_with_assist_1_player_id	tackle_with_assist_1_player_name	tackle_with_assist_1_team
                        tackle_with_assist_2_player_id	tackle_with_assist_2_player_name	tackle_with_assist_2_team
                        pass_defense_1_player_id	pass_defense_1_player_name	pass_defense_2_player_id	pass_defense_2_player_name
                        fumbled_1_team	fumbled_1_player_id	fumbled_1_player_name	fumbled_2_player_id	fumbled_2_player_name	fumbled_2_team
                        fumble_recovery_1_team	fumble_recovery_1_yards	fumble_recovery_1_player_id	fumble_recovery_1_player_name
                        fumble_recovery_2_team	fumble_recovery_2_yards	fumble_recovery_2_player_id	fumble_recovery_2_player_name	sack_player_id	sack_player_name
                        half_sack_1_player_id	half_sack_1_player_name	half_sack_2_player_id	half_sack_2_player_name	return_team	return_yards
                        penalty_team	penalty_player_id	penalty_player_name	penalty_yards	replay_or_challenge	replay_or_challenge_result
                        penalty_type	defensive_two_point_attempt	defensive_two_point_conv	defensive_extra_point_attempt	defensive_extra_point_conv
                        safety_player_name	safety_player_id
                        series	series_success	series_result	order_sequence	start_time	time_of_day
                        stadium	weather,
                        special_teams_play	st_play_type	end_clock_time	end_yard_line
                        fixed_drive	fixed_drive_result	drive_real_start_time	drive_play_count	drive_time_of_possession	drive_first_downs	drive_inside20
                        drive_ended_with_score	drive_quarter_start	drive_quarter_end	drive_yards_penalized	drive_start_transition	drive_end_transition
                        drive_game_clock_start	drive_game_clock_end	drive_start_yard_line	drive_end_yard_line	drive_play_id_started	drive_play_id_ended
                        away_score	home_score	location	result	total	spread_line	total_line	div_game	roof	surface	temp	wind	home_coach	away_coach
                        stadium_id	game_stadium	aborted_play	success	out_of_bounds	home_opening_kickoff

                    },
                    index=[0]
                )
