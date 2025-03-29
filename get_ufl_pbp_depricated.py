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
    plays_arr = []
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

                score_differential = \
                    posteam_score_post - defteam_score_post
                posteam_score = posteam_score_post
                defteam_score = defteam_score_post
                # Variables that have to be cleared every play.
                # quarter_end = 0
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
                first_down_pass = 0
                is_replay_or_challenge = 0
                is_return_touchdown = 0
                third_down_converted = 0
                third_down_failed = 0
                fourth_down_converted = 0
                fourth_down_failed = 0
                is_kickoff_attempt = 0
                is_kickoff_out_of_bounds = 0
                is_kickoff_inside_twenty = 0
                is_kickoff_in_endzone = 0
                is_kickoff_downed = 0
                is_kickoff_fair_catch = 0
                is_special_teams_play = 0
                is_scrimmage_play = 0
                penalty_yards = 0
                first_down_penalty = 0
                is_fumble_forced = 0
                is_fumble_not_forced = 0
                is_fumble_out_of_bounds = 0
                is_fumble_lost = 0
                fumble_recovery_1_yards = 0
                rushing_yards = 0
                is_tackled_for_loss = 0
                first_down_rush = 0
                is_rush_touchdown = 0
                passing_yards = 0
                receiving_yards = 0
                play_quarter_num = 0

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
                assist_tackle_1_player_name = ""
                assist_tackle_2_team = ""
                assist_tackle_2_player_name = ""
                replay_or_challenge_result = ""
                kickoff_returner_player_name = ""
                return_team = ""
                interception_player_name = ""
                solo_tackle_2_team = ""
                solo_tackle_2_player_name = ""
                assist_tackle_3_team = ""
                assist_tackle_4_team = ""
                assist_tackle_3_player_name = ""
                assist_tackle_4_player_name = ""
                penalty_team = ""
                penalty_player_name = ""
                penalty_type = ""
                pass_defense_1_player_name = ""
                pass_defense_2_player_name = ""
                forced_fumble_player_1_team = ""
                forced_fumble_player_1_player_name = ""
                fumbled_1_team = ""
                fumbled_1_player_name = ""
                fumble_recovery_1_team = ""
                fumble_recovery_1_player_name = ""
                rusher_player_name = ""
                tackle_for_loss_1_player_name = ""
                tackle_for_loss_2_player_name = ""
                sack_player_name = ""
                half_sack_1_player_name = ""
                half_sack_2_player_name = ""
                solo_tackle_1_player_name = ""

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

                # Play type
                if "no play" in desc.lower():
                    play_type = "no_play"
                elif "tv timeout" in desc.lower():
                    play_type = "no_play"
                elif "timeout #" in desc.lower():
                    play_type = "no_play"
                elif "(aborted)" in desc.lower() \
                        and "pass" in desc.lower():
                    play_type = "pass"
                    is_aborted_play = 1
                    is_scrimmage_play = 1

                    check = re.findall(
                        r"([[a-zA-Z\'\.\-]+) FUMBLES \(aborted\). " +
                        r"Fumble RECOVERED by " +
                        r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]

                    is_fumble_not_forced = 1

                    fumble_recovery_1_team = check[0][1]
                    fumble_recovery_1_player_name = check[0][2]
                    fum_yd_line_temp = check[0][3]
                    fum_yd_line = get_yardline(
                        fum_yd_line_temp,
                        posteam
                    )

                    if fumble_recovery_1_team == posteam:
                        is_fumble_lost = 0
                        fumble_recovery_1_yards = yardline_100 -\
                            fum_yd_line
                    elif fumble_recovery_1_team == defteam:
                        raise ValueError(
                            "Please implement this logic."
                        )
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"{fumble_recovery_1_team}"
                        )
                elif "(aborted)" in desc.lower() \
                        and "rush" in desc.lower():
                    play_type = "run"
                    is_aborted_play = 1
                    is_scrimmage_play = 1

                    check = re.findall(
                        r"([[a-zA-Z\'\.\-]+) FUMBLES \(aborted\). " +
                        r"Fumble RECOVERED by " +
                        r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]

                    is_fumble_not_forced = 1

                    fumble_recovery_1_team = check[0][1]
                    fumble_recovery_1_player_name = check[0][2]
                    fum_yd_line_temp = check[0][3]
                    fum_yd_line = get_yardline(
                        fum_yd_line_temp,
                        posteam
                    )

                    if fumble_recovery_1_team == posteam:
                        is_fumble_lost = 0
                        fumble_recovery_1_yards = yardline_100 -\
                            fum_yd_line
                    elif fumble_recovery_1_team == defteam:
                        raise ValueError(
                            "Please implement this logic."
                        )
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"{fumble_recovery_1_team}"
                        )
                elif "pass" in desc.lower():
                    play_type = "pass"
                    is_qb_dropback = 1
                    is_pass_play = 1
                    is_pass_attempt = 1
                    is_scrimmage_play = 1
                elif "spike" in desc.lower():
                    play_type = "qb_spike"
                    is_qb_dropback = 1
                    is_qb_spike = 1
                    is_pass_play = 1
                    is_pass_attempt = 1
                    is_scrimmage_play = 1
                elif "rushed" in desc.lower():
                    play_type = "run"
                    is_rush_play = 1
                    is_rush_attempt = 1
                    is_scrimmage_play = 1
                elif "scramble" in desc.lower():
                    play_type = "run"
                    is_qb_dropback = 1
                    is_qb_scramble = 1
                    is_rush_play = 1
                    is_rush_attempt = 1
                    is_scrimmage_play = 1
                elif "kneel" in desc.lower():
                    play_type = "qb_kneel"
                    is_qb_kneel = 1
                    is_rush_play = 1
                    is_rush_attempt = 1
                    is_scrimmage_play = 1
                elif "field goal" in desc.lower():
                    play_type = "field_goal"
                    is_special_teams_play = 1
                elif "kickoff" in desc.lower() \
                        or "KICKOFF" in down_and_distance_temp.lower():
                    play_type = "kickoff"
                    is_special_teams_play = 1
                elif "kicks" in desc.lower():
                    play_type = "punt"
                    is_special_teams_play = 1
                elif "punt" in desc.lower():
                    play_type = "punt"
                    is_special_teams_play = 1
                elif "end quarter" in desc.lower():
                    play_type = ""
                elif "two minute warning." == desc.lower():
                    play_type = ""
                elif "end game" == desc.lower():
                    play_type = ""
                elif "aborted" in desc.lower():
                    play_type = "rush"
                    is_scrimmage_play = 1
                elif "penalty" in desc.lower():
                    play_type = "no_play"
                elif "extra point" in desc.lower():
                    play_type
                    is_special_teams_play = 1
                    play_type = "extra_point"
                elif "timeout the replay official" in desc.lower():
                    play_type = "no_play"
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
                elif play_type == "run" \
                        and "-point attempt" in desc.lower():
                    run_location = ""
                    run_gap = ""
                elif play_type == "run" and "rushed to" in desc.lower():
                    run_location = ""
                    run_gap = ""
                elif play_type == "run" \
                        and "rushed reverse to" in desc.lower():
                    run_location = ""
                    run_gap = ""
                elif play_type == "run" \
                        and "scrambles to" in desc.lower():
                    run_location = ""
                    run_gap = ""
                elif play_type == "run":
                    raise ValueError(
                        f"Unhandled play `{desc}`"
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
                elif play_type == "pass" \
                        and "replay official" in desc.lower():
                    pass_length = ""
                    pass_location = ""
                    is_replay_or_challenge = 1
                elif play_type == "pass" \
                        and "pass complete to" in desc.lower():
                    pass_length = ""
                    pass_location = ""
                elif play_type == "pass" \
                        and "pass incomplete intended for" in desc.lower():
                    pass_length = ""
                    pass_location = ""
                elif play_type == "pass" \
                        and "middle intended" in desc.lower():
                    pass_length = "middle"
                    pass_location = ""
                elif play_type == "pass" \
                        and "rushed backward pass" in desc.lower():
                    play_type = "run"
                    pass_length = ""
                    pass_location = ""
                elif play_type == "pass":
                    raise ValueError(
                        f"Unhandled play `{desc}`"
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

                if "touchback" in desc.lower():
                    is_touchback = 1

                if "no huddle" in desc.lower():
                    no_huddle = 1

                if "qb hit" in desc.lower():
                    is_qb_hit = 1

                if "field goal" in desc.lower():
                    is_field_goal_attempt = 1
                    check = re.findall(
                            r"([a-zA-Z]+\.[a-zA-Z]+)( \d\d)? " +
                            r"yard field goal attempt is ([a-zA-Z\s]+),",
                            desc
                        )
                    kicker_player_name = check[0][0]
                    try:
                        kick_distance = int(check[0][1])
                        field_goal_result = check[0][2].lower()
                    except Exception:
                        kick_distance = None
                        field_goal_result = check[0][1].lower()

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
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass incomplete " +
                        r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.",
                        desc
                    )
                    passer_player_name = check[0][0]
                    receiver_player_name = check[0][1]
                    is_incomplete_pass = 1
                    if (("(" in desc.lower()) or (")" in desc.lower())) \
                            and "sack" not in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"pass incomplete " +
                            r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)? " +
                            r"\(([a-zA-Z]\.[a-zA-Z\'\-\s]+)\).",
                            desc
                        )
                        passer_player_name = check[0][0]
                        receiver_player_name = check[0][1]
                        pd_temp = check[0][2]

                        if "," in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ","
                                )
                        elif ";" in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ";"
                                )
                        else:
                            pass_defense_1_player_name = pd_temp

                    del check
                elif "pass" in desc.lower() \
                        and "incomplete" in desc.lower()\
                        and "steps back to pass. pass incomplete" \
                            in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                        r"steps back to pass. pass incomplete " +
                        r"([a-zA-Z]+) ([a-zA-Z]+)? " +
                        r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.",
                        desc
                    )
                    passer_player_name = check[0][0]
                    receiver_player_name = check[0][3]
                    is_incomplete_pass = 1

                    if (("(" in desc.lower()) or (")" in desc.lower())) \
                            and "sack" not in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"steps back to pass. pass incomplete " +
                            r"([a-zA-Z]+) ([a-zA-Z]+)? " +
                            r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)? " +
                            r"(\(([a-zA-Z\'\.\,\-\s]+)\))?.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        receiver_player_name = check[0][3]
                        try:
                            pd_temp = check[0][4]
                        except Exception:
                            pd_temp = ""

                        if pd_temp == "":
                            pass
                        elif "," in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ","
                                )
                        elif ";" in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ";"
                                )
                        else:
                            pass_defense_1_player_name = pd_temp

                    del check
                elif "pass" in desc.lower() \
                        and "incomplete" in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass incomplete" +
                        r"( [a-zA-Z]+)( [a-zA-Z]+)? " +
                        r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?.",
                        desc
                    )
                    passer_player_name = check[0][0]
                    receiver_player_name = check[0][3]
                    is_incomplete_pass = 1

                    if (("(" in desc.lower()) or (")" in desc.lower())) \
                            and "sack" not in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"pass incomplete" +
                            r"( [a-zA-Z]+)?( [a-zA-Z]+)? " +
                            r"intended for( [a-zA-Z].[a-zA-Z\'\-]+)?" +
                            r"( \(([a-zA-Z\'\.\,\-\s]+)\))?.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        receiver_player_name = check[0][3]
                        try:
                            pd_temp = check[0][4]
                        except Exception:
                            pd_temp = ""

                        if pd_temp == "":
                            pass
                        elif "," in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ","
                                )
                        elif ";" in pd_temp:
                            pass_defense_1_player_name, \
                                pass_defense_2_player_name = pd_temp.split(
                                    ";"
                                )
                        else:
                            pass_defense_1_player_name = pd_temp

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
                        receiving_yards = passing_yards
                        yards_gained = 0
                    else:
                        yards_gained = int(yards_gained)
                        air_yards = pass_yardline_num - yardline_100
                        yards_after_catch = yardline_100 - (
                            yardline_100 - yards_gained
                        )
                        passing_yards = yards_gained

                    if "touchdown" in desc.lower():
                        is_pass_touchdown = 1

                    if yards_gained > yds_to_go \
                            and "gain of yards" not in desc.lower():
                        first_down_pass = 1
                        is_first_down = 1
                        drive_first_downs += 1

                    if "tackled by" in desc.lower():
                        check = re.findall(
                            r"Tackled by " +
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                            desc
                        )
                        tacklers_temp = check[0][0]
                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")

                        if "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")

                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = defteam
                            solo_tackle_1_player_name = tacklers_temp

                    elif "pushed out" in desc.lower():
                        check = re.findall(
                            r"Pushed out of bounds by " +
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                            desc
                        )
                        tacklers_temp = check[0][0]
                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")

                        if "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")

                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = defteam
                            solo_tackle_1_player_name = tacklers_temp

                    del check
                elif "pass" in desc.lower() and "complete" in desc.lower():
                    is_complete_pass = 1
                    check = re.findall(
                        r"([a-zA-Z][a-zA-Z\.\'\-\s]+) pass" +
                        r"( [a-zA-Z]+)?( [a-zA-Z]+)? complete to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                        r"Catch made by ([a-zA-Z\.\'\-\s]+) at " +
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
                        yards_gained = 0
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

                    if yards_gained > yds_to_go \
                            and "gain of yards" not in desc.lower():
                        first_down_pass = 1
                        is_first_down = 1
                        drive_first_downs += 1

                    del check
                elif "pass" in desc.lower() \
                        and "intercepted" in desc.lower():
                    is_incomplete_pass = 1
                    is_interception = 1
                    if "pass intercepted" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass " +
                            r"INTERCEPTED at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Intercepted by " +
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        interception_player_name = check[0][2]
                        int_ret_start_temp = check[0][3]
                    else:
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) pass " +
                            r"([a-zA-Z]+) ([a-zA-Z]+) INTERCEPTED at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Intercepted by " +
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        passer_player_name = check[0][0]
                        interception_player_name = check[0][4]
                        int_ret_start_temp = check[0][5]
                    int_ret_start = get_yardline(
                        int_ret_start_temp,
                        posteam
                    )

                    if "touchdown" in desc.lower():
                        is_return_touchdown = 1
                        return_yards = 100 - int_ret_start
                    elif "touchback" in desc.lower():
                        return_yards = 0 - int_ret_start
                    elif "tackled by at" in desc.lower():
                        # This means there is no designated tackler
                        # for this play for some reason.
                        pass
                    elif "tackled by" in desc.lower():
                        check = re.findall(
                            r"Tackled by ([a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        tacklers_temp = check[0][0]
                        int_ret_end_temp = check[0][1]

                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_3_team = posteam
                            assist_tackle_4_team = posteam
                            assist_tackle_3_player_name, \
                                assist_tackle_4_player_name = \
                                tacklers_temp.split("; ")
                        elif "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_3_team = posteam
                            assist_tackle_4_team = posteam
                            assist_tackle_3_player_name, \
                                assist_tackle_4_player_name = \
                                tacklers_temp.split(", ")
                        else:
                            is_solo_tackle = 1
                            solo_tackle_2_team = posteam
                            solo_tackle_2_player_name = tacklers_temp

                        int_ret_end = get_yardline(
                            int_ret_end_temp,
                            posteam
                        )
                        return_yards = int_ret_end - int_ret_start
                        del tacklers_temp
                        del int_ret_end
                        del int_ret_end_temp
                    elif "pushed out of bounds" in desc.lower():
                        check = re.findall(
                            r"Pushed out of bounds by " +
                            r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        tacklers_temp = check[0][0]
                        int_ret_end_temp = check[0][1]

                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_3_team = posteam
                            assist_tackle_4_team = posteam
                            assist_tackle_3_player_name, \
                                assist_tackle_4_player_name = \
                                tacklers_temp.split("; ")
                        elif "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_3_team = posteam
                            assist_tackle_4_team = posteam
                            assist_tackle_3_player_name, \
                                assist_tackle_4_player_name = \
                                tacklers_temp.split(", ")
                        else:
                            is_solo_tackle = 1
                            solo_tackle_2_team = posteam
                            solo_tackle_2_player_name = tacklers_temp

                        int_ret_end = get_yardline(
                            int_ret_end_temp,
                            posteam
                        )
                        return_yards = int_ret_end - int_ret_start
                        del tacklers_temp
                        del int_ret_end
                        del int_ret_end_temp
                    elif "ran out of bounds" in desc.lower():
                        # Edge case found in Game ID #9
                        return_yards = 0
                    else:

                        pass
                        logging.warning(
                            f"Unusual play `{desc}`\n" +
                            "Double check play for unusual outcomes."
                        )
                    del int_ret_start
                    del int_ret_start_temp
                elif "pass" in desc.lower() \
                        and "sack" in desc.lower() \
                        and "for yards " in desc.lower():
                    is_sack = 1
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) sacked at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"yards \(([a-zA-Z\;\s\.]+)\)",
                        desc
                    )
                    passer_player_name = check[0][0]
                    yards_gained = 0
                    sack_temp = check[0][2]

                    if ";" in sack_temp:
                        half_sack_1_player_name, half_sack_2_player_name \
                            = sack_temp.split("; ")
                    elif "," in sack_temp:
                        half_sack_1_player_name, half_sack_2_player_name \
                            = sack_temp.split(", ")
                    else:
                        sack_player_name = sack_temp
                elif "pass" in desc.lower() and "sack" in desc.lower():
                    is_sack = 1
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) sacked at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"([0-9\-]+) yards \(([a-zA-Z\;\s\.\'\-]+)\)",
                        desc
                    )
                    passer_player_name = check[0][0]
                    yards_gained = check[0][2]
                    yards_gained = int(yards_gained)
                    sack_temp = check[0][3]

                    if ";" in sack_temp:
                        half_sack_1_player_name, half_sack_2_player_name \
                            = sack_temp.split("; ")
                    elif "," in sack_temp:
                        half_sack_1_player_name, half_sack_2_player_name \
                            = sack_temp.split(", ")
                    else:
                        sack_player_name = sack_temp

                if "touchdown" in desc.lower() \
                        and "Gain of yards. for yards, TOUCHDOWN." \
                            in desc.lower():
                    pass
                if "rush" in desc.lower() \
                        and "for yards." in desc.lower() \
                        and "up the middle" not in desc.lower() \
                        and "-point conversion " not in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) rushed" +
                        r"( [a-zA-Z]+)?( [a-zA-Z]+)? to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"yards.",
                        desc
                    )
                    rusher_player_name = check[0][0]
                    rushing_yards = 0
                    if "touchdown" in desc.lower() \
                            and "fumble" not in desc.lower():
                        is_rush_touchdown = 1
                elif "rush" in desc.lower() \
                        and "for yards." in desc.lower() \
                        and "up the middle" in desc.lower() \
                        and "-point conversion " not in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) rushed " +
                        r"up the middle to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"yards.",
                        desc
                    )
                    rusher_player_name = check[0][0]
                    rushing_yards = 0
                    if "touchdown" in desc.lower() \
                            and "fumble" not in desc.lower():
                        is_rush_touchdown = 1
                elif "rush" in desc.lower() \
                        and "up the middle" not in desc.lower() \
                        and "-point conversion " not in desc.lower():

                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) rushed" +
                        r"( [a-zA-Z]+)?( [a-zA-Z]+)? to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"([0-9\-]+) yards.",
                        desc
                    )
                    rusher_player_name = check[0][0]
                    # rush_start_temp = check[0][2]
                    rushing_yards = check[0][4]
                    rushing_yards = int(rushing_yards)

                    if "touchdown" in desc.lower() \
                            and "fumble" not in desc.lower():
                        is_rush_touchdown = 1

                    if rushing_yards > yds_to_go:
                        first_down_rush = 1
                        is_first_down = 1
                elif "rush" in desc.lower() \
                        and "up the middle" in desc.lower() \
                        and "-point conversion " not in desc.lower():

                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) rushed " +
                        r"up the middle to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for " +
                        r"([0-9\-]+) yards.",
                        desc
                    )
                    rusher_player_name = check[0][0]
                    # rush_start_temp = check[0][1]
                    rushing_yards = check[0][2]
                    rushing_yards = int(rushing_yards)

                    if "touchdown" in desc.lower() \
                            and "fumble" not in desc.lower():
                        is_rush_touchdown = 1

                    if rushing_yards > yds_to_go:
                        first_down_rush = 1
                        is_first_down = 1

                if rushing_yards < 0:
                    is_tackled_for_loss = 1

                    if "tackled by at" in desc.lower():
                        # edge case found in game ID #3,
                        # where it says someone made a tackle,
                        # but there's no player named for the tackle.
                        pass
                    elif "tackled by" in desc.lower():
                        check = re.findall(
                            r"Tackled by " +
                            r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                            desc
                        )
                        tacklers_temp = check[0][0]

                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")
                            tackle_for_loss_1_player_name, \
                                tackle_for_loss_2_player_name = \
                                tacklers_temp.split("; ")
                        elif "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")
                            tackle_for_loss_1_player_name, \
                                tackle_for_loss_2_player_name = \
                                tacklers_temp.split(", ")
                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = defteam
                            solo_tackle_1_player_name = tacklers_temp
                            tackle_for_loss_1_player_name = \
                                solo_tackle_1_player_name
                    elif "pushed out" in desc.lower():
                        check = re.findall(
                            r"Pushed out of bounds by " +
                            r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                            desc
                        )
                        tacklers_temp = check[0][0]
                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")
                            tackle_for_loss_1_player_name, \
                                tackle_for_loss_2_player_name = \
                                tacklers_temp.split("; ")
                        elif "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = defteam
                            assist_tackle_2_team = defteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")
                            tackle_for_loss_1_player_name, \
                                tackle_for_loss_2_player_name = \
                                tacklers_temp.split(", ")
                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = defteam
                            solo_tackle_1_player_name = tacklers_temp
                            tackle_for_loss_1_player_name = \
                                solo_tackle_1_player_name

                if "safety" in desc.lower():
                    is_safety = 1

                if "punts yards" in desc.lower():
                    # Yes, there is a play where it's written
                    # "{player} punts yards to {yardline}"

                    # It appears to be an edge case when
                    # someone punts twice in one play.
                    # This is to catch that edge case.
                    check = re.findall(
                        r"([a-zA-Z\'\-\s]+) punts " +
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
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) punts ([0-9\-]+) " +
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
                    return_team = defteam

                    if "downed" in desc.lower():
                        is_punt_downed = 1
                    elif "downed" in desc.lower() \
                            and kick_to_yd_line < 20:
                        is_punt_downed = 1
                        is_punt_inside_twenty = 1

                    if "fair catch" in desc.lower() \
                            and kick_to_yd_line < 20:
                        check = re.findall(
                            r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-\s]+).",
                            desc
                        )
                        punt_returner_player_name = check[0][0]
                        is_punt_fair_catch = 1
                        del check
                    elif "fair catch" in desc.lower():
                        check = re.findall(
                            r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-\s]+).",
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
                        del check

                    if "touchback" in desc.lower():
                        is_punt_in_endzone = 1

                    if "out of bounds" in desc.lower():
                        is_punt_out_of_bounds = 1
                    elif "out of bounds" in desc.lower():
                        is_punt_out_of_bounds = 1
                        is_punt_inside_twenty = 1

                    if "tackled by" in desc.lower() \
                            and "muff" not in desc.lower() \
                            and "fumble" not in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"returned punt from " +
                            r"the ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Tackled by " +
                            r"([a-zA-Z\'\-\s\,\.\;]+) at " +
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

                        if punt_return_end_num < 20:
                            is_punt_inside_twenty = 1
                        return_yards = punt_return_end_num - \
                            punt_return_start_num

                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")

                        if "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")

                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = posteam
                            solo_tackle_1_player_name = tacklers_temp

                        del check
                    elif "pushed out" in desc.lower() \
                            and "muff" not in desc.lower() \
                            and "fumble" not in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"returned punt from the " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+). " +
                            r"Pushed out of bounds by " +
                            r"([a-zA-Z\'\-\s\,\.\;]+) at " +
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

                        if punt_return_end_num < 20:
                            is_punt_inside_twenty = 1

                        return_yards = punt_return_end_num - \
                            punt_return_start_num

                        if ";" in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split("; ")

                        if "," in tacklers_temp:
                            is_assist_tackle = 1
                            tackle_with_assist = 1
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            assist_tackle_1_player_name, \
                                assist_tackle_2_player_name = \
                                tacklers_temp.split(", ")

                        else:
                            is_solo_tackle = 1
                            solo_tackle_1_team = posteam
                            solo_tackle_1_player_name = tacklers_temp

                        del check

                    if "touchdown" in desc.lower():
                        is_return_touchdown = 1
                    # is_punt_inside_twenty = 1

                if "replay official" in desc.lower():
                    check = re.findall(
                        r"The Replay Official reviewed ([a-zA-Z\s]+) " +
                        r"and the play was ([a-zA-Z\s]+)\.",
                        desc
                    )
                    is_replay_or_challenge = 1
                    replay_or_challenge_result = check[0][1]
                    del check
                elif "challenged" in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]+) challenged ([a-zA-Z\s]+) " +
                        r"and the play was ([a-zA-Z\s]+)\.",
                        desc
                    )
                    is_replay_or_challenge = 1
                    replay_or_challenge_result = check[0][2]
                    del check

                if "tackled by at" in desc.lower():
                    # edge case found in game ID #3,
                    # where it says someone made a tackle,
                    # but there's no player named for the tackle.
                    pass
                elif "tackled by" in desc.lower():
                    check = re.findall(
                        r"Tackled by " +
                        r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                        desc
                    )
                    tacklers_temp = check[0][0]
                    if ";" in tacklers_temp:
                        is_assist_tackle = 1
                        tackle_with_assist = 1
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                        assist_tackle_1_player_name, \
                            assist_tackle_2_player_name = \
                            tacklers_temp.split("; ")
                    elif "," in tacklers_temp:
                        is_assist_tackle = 1
                        tackle_with_assist = 1
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                        assist_tackle_1_player_name, \
                            assist_tackle_2_player_name = \
                            tacklers_temp.split(", ")
                    else:
                        is_solo_tackle = 1
                        solo_tackle_1_team = defteam
                        solo_tackle_1_player_name = tacklers_temp
                elif "pushed out of bounds by at" in desc.lower():
                    pass
                elif "pushed out" in desc.lower():
                    check = re.findall(
                        r"Pushed out of bounds by " +
                        r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                        desc
                    )
                    tacklers_temp = check[0][0]
                    if ";" in tacklers_temp:
                        is_assist_tackle = 1
                        tackle_with_assist = 1
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                        assist_tackle_1_player_name, \
                            assist_tackle_2_player_name = \
                            tacklers_temp.split("; ")
                    elif "," in tacklers_temp:
                        is_assist_tackle = 1
                        tackle_with_assist = 1
                        assist_tackle_1_team = defteam
                        assist_tackle_2_team = defteam
                        assist_tackle_1_player_name, \
                            assist_tackle_2_player_name = \
                            tacklers_temp.split(", ")
                    else:
                        is_solo_tackle = 1
                        solo_tackle_1_team = defteam
                        solo_tackle_1_player_name = tacklers_temp

                if play_type == "kickoff" \
                        and "kicks yards from " in desc.lower():
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) kicks yards from " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) to the " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                        desc
                    )
                    kicker_player_name = check[0][0]
                    return_loc_start_temp = check[0][1]
                    return_team = posteam

                    if "out of bounds" in desc.lower():
                        is_kickoff_out_of_bounds = 1

                    if "fair catch" in desc.lower():
                        check = re.findall(
                            r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-\s]+).",
                            desc
                        )

                        kickoff_returner_player_name = check[0][0]
                        is_kickoff_fair_catch = 1
                    if "touchback" in desc.lower():
                        is_kickoff_in_endzone = 1

                    if "downed" in desc.lower():
                        is_kickoff_downed = 1

                    if "returns the kickoff" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"returns the kickoff\.",
                            desc
                        )
                        if "tackled by at" in desc.lower():
                            # edge case found in game ID #3,
                            # where it says someone made a tackle,
                            # but there's no player named for the tackle.
                            pass
                        elif "tackled by" in desc.lower():
                            check = re.findall(
                                r"Tackled by " +
                                r"([a-zA-Z]\.[a-zA-Z\'\-\s\,\.\;]+) at " +
                                r"([a-zA-Z]+\s[0-9]+|" +
                                r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)",
                                desc
                            )
                            return_loc_end_temp = check[0][1]
                            kr_start = get_yardline(
                                return_loc_start_temp,
                                posteam
                            )

                            kr_end = get_yardline(
                                return_loc_end_temp,
                                posteam
                            )

                            return_yards = kr_end - kr_start

                            if kr_end < 20:
                                is_kickoff_inside_twenty = 1

                            del return_loc_start_temp
                            del return_loc_end_temp

                elif play_type == "kickoff":
                    check = re.findall(
                        r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) kicks ([0-9]+) " +
                        r"yards from ([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) to the " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+).",
                        desc
                    )
                    kicker_player_name = check[0][0]
                    kick_distance = check[0][1]
                    return_loc_start_temp = check[0][2]
                    return_team = posteam

                    if "out of bounds" in desc.lower():
                        is_kickoff_out_of_bounds = 1

                    if "fair catch" in desc.lower():
                        check = re.findall(
                            r"Fair catch by ([a-zA-Z]\.[a-zA-Z\'\-\s]+).",
                            desc
                        )

                        kickoff_returner_player_name = check[0][0]
                        is_kickoff_fair_catch = 1

                    if "touchback" in desc.lower():
                        is_kickoff_in_endzone = 1

                    if "downed" in desc.lower():
                        is_kickoff_downed = 1

                    if "returns the kickoff" in desc.lower():
                        check = re.findall(
                            r"([a-zA-Z]\.[a-zA-Z\'\-\s]+) " +
                            r"returns the kickoff\.",
                            desc
                        )
                        if "tackled by at" in desc.lower():
                            # edge case found in game ID #3,
                            # where it says someone made a tackle,
                            # but there's no player named for the tackle.
                            pass
                        elif "tackled by" in desc.lower():
                            check = re.findall(
                                r"Tackled by " +
                                r"([a-zA-Z\'\-\s\,\.\;]+) at " +
                                r"([a-zA-Z]+\s[0-9]+|" +
                                r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)",
                                desc
                            )
                            return_loc_end_temp = check[0][1]
                            kr_start = get_yardline(
                                return_loc_start_temp,
                                posteam
                            )

                            kr_end = get_yardline(
                                return_loc_end_temp,
                                posteam
                            )

                            return_yards = kr_end - kr_start

                            if kr_end < 20:
                                is_kickoff_inside_twenty = 1

                            del return_loc_start_temp
                            del return_loc_end_temp

                if down == 3 and yards_gained >= yds_to_go:
                    third_down_converted = 1
                elif down == 3 and yards_gained < yds_to_go:
                    third_down_failed = 1
                elif down == 4 and yards_gained >= yds_to_go:
                    fourth_down_converted = 1
                elif down == 4 and yards_gained < yds_to_go:
                    fourth_down_failed = 1

                if "fumbles, out of bounds" in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "muff" not in desc.lower()\
                        and "pushed out of bounds" not in desc.lower():
                    check = re.findall(
                        r"([[a-zA-Z\'\.\-\s]+) FUMBLES, [oO]ut of bounds.",
                        desc
                    )

                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    # forced_fumble_player_1_team = defteam
                    # forced_fumble_player_1_player_name = check[0][1]

                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "ran out of bounds" in desc.lower()\
                        and "muff" not in desc.lower()\
                        and "pushed out of bounds" not in desc.lower():
                    check = re.findall(
                        r"([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                        r"([[a-zA-Z\'\.\-\s]+). Fumble RECOVERED by " +
                        r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)." +
                        r"([[a-zA-Z\'\.\-\s]+) ran out of bounds.",
                        desc
                    )

                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]
                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "out of bounds" in desc.lower()\
                        and "muff" not in desc.lower()\
                        and "pushed out of bounds" not in desc.lower():
                    check = re.findall(
                        r"([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                        r"([[a-zA-Z\'\.\-\s]+). [oO]ut of bounds.",
                        desc
                    )

                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]
                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "rushed" in desc.lower()\
                        and "tackled by at " in desc.lower()\
                        and "two-point conversion attempt" in desc.lower()\
                        and "recovers the fumble." in desc.lower():
                    is_fumble_forced = 0
                    is_fumble_not_forced = 0
                    is_fumble_out_of_bounds = 0
                    is_fumble_lost = 0
                    fumble_recovery_1_yards = 0

                    forced_fumble_player_1_team = ""
                    forced_fumble_player_1_player_name = ""
                    fumble_recovery_1_team = ""
                    fumble_recovery_1_player_name = ""

                    check = re.findall(
                        r"TWO-POINT CONVERSION ATTEMPT. " +
                        r"([[a-zA-Z\'\.\-\s]+) rushed" +
                        r"( [a-zA-Z]+)?( [a-zA-Z]+)? to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for yards\." +
                        r"( [[a-zA-Z\'\.\-\s]+)? FUMBLES, forced by " +
                        r"([a-zA-Z\'\.\-\s]+). ([a-zA-Z\'\.\-\s]+) " +
                        r"recovers the fumble. Tackled by at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]

                    fumble_recovery_1_team = posteam
                    fumble_recovery_1_player_name = check[0][2]
                    fum_start = yardline_100

                    if fumble_recovery_1_team == posteam:
                        fumble_recovery_1_yards = 0
                    elif fumble_recovery_1_team == defteam:
                        fumble_recovery_1_yards = 0
                        is_fumble_lost = 1
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"`{fumble_recovery_1_team}`"
                        )

                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "rushed" in desc.lower()\
                        and "two-point conversion attempt" in desc.lower()\
                        and "recovers the fumble." in desc.lower():
                    is_fumble_forced = 0
                    is_fumble_not_forced = 0
                    is_fumble_out_of_bounds = 0
                    is_fumble_lost = 0
                    fumble_recovery_1_yards = 0

                    forced_fumble_player_1_team = ""
                    forced_fumble_player_1_player_name = ""
                    fumble_recovery_1_team = ""
                    fumble_recovery_1_player_name = ""

                    check = re.findall(
                        r"TWO-POINT CONVERSION ATTEMPT. " +
                        r"([[a-zA-Z\'\.\-\s]+) rushed" +
                        r"( [a-zA-Z]+)?( [a-zA-Z]+)? to " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+) for yards\." +
                        r"( [[a-zA-Z\'\.\-\s]+)? FUMBLES, forced by " +
                        r"([[a-zA-Z\'\.\-\s]+). ([[a-zA-Z\'\.\-\s]+) " +
                        r"recovers the fumble. Tackled by " +
                        r"([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]

                    fumble_recovery_1_team = posteam
                    fumble_recovery_1_player_name = check[0][2]
                    fum_start = yardline_100

                    if fumble_recovery_1_team == posteam:
                        fumble_recovery_1_yards = 0
                    elif fumble_recovery_1_team == defteam:
                        fumble_recovery_1_yards = 0
                        is_fumble_lost = 1
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"`{fumble_recovery_1_team}`"
                        )

                    if "tackled by at" in desc.lower() \
                            and "replay" not in desc.lower():
                        # Edge case found in game ID #7
                        check = re.findall(
                            r" ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                            r"([[a-zA-Z\'\.\-\s]+) Fumble RECOVERED by " +
                            r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Tackled by at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][5]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end

                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "recovers the fumble." in desc.lower():
                    is_fumble_forced = 0
                    is_fumble_not_forced = 0
                    is_fumble_out_of_bounds = 0
                    is_fumble_lost = 0
                    fumble_recovery_1_yards = 0

                    forced_fumble_player_1_team = ""
                    forced_fumble_player_1_player_name = ""
                    fumble_recovery_1_team = ""
                    fumble_recovery_1_player_name = ""

                    check = re.findall(
                        r"\. ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                        r"([[a-zA-Z\'\.\-\s]+). ([[a-zA-Z\'\.\-\s]+) " +
                        r"recovers the fumble. " +
                        r"Tackled by ([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]

                    fumble_recovery_1_team = posteam
                    fumble_recovery_1_player_name = check[0][2]
                    # fum_start_temp = check[0][4]
                    fum_start = yardline_100

                    if fumble_recovery_1_team == posteam:
                        fumble_recovery_1_yards = 0
                    elif fumble_recovery_1_team == defteam:
                        fumble_recovery_1_yards = 0
                        is_fumble_lost = 1
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"`{fumble_recovery_1_team}`"
                        )

                    if "tackled by at" in desc.lower() \
                            and "replay" not in desc.lower():
                        # Edge case found in game ID #7
                        check = re.findall(
                            r" ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                            r"([[a-zA-Z\'\.\-\s]+) Fumble RECOVERED by " +
                            r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Tackled by at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][5]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end
                    elif "tackled" in desc.lower():
                        check = re.findall(
                            r"Tackled by ([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][1]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end

                    elif "out of bounds" in desc.lower():
                        is_fumble_out_of_bounds = 1
                    elif "touchdown" in desc.lower():
                        is_return_touchdown = 1
                        return_yards = fum_start
                    elif "recovered by" in desc.lower():
                        pass
                    else:
                        raise ValueError(
                            "Unhandled play desc " +
                            f"`{desc}`"
                        )

                    # del fum_start_temp

                elif "fumbles, forced by." in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "muff" not in desc.lower():
                    is_fumble_forced = 0
                    is_fumble_not_forced = 1
                    is_fumble_out_of_bounds = 0
                    is_fumble_lost = 0
                    fumble_recovery_1_yards = 0

                    forced_fumble_player_1_team = ""
                    forced_fumble_player_1_player_name = ""
                    fumble_recovery_1_team = ""
                    fumble_recovery_1_player_name = ""

                    check = re.findall(
                        r". ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by. " +
                        r"Fumble RECOVERED by " +
                        r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    # forced_fumble_player_1_team = defteam
                    # forced_fumble_player_1_player_name = check[0][1]

                    fumble_recovery_1_team = check[0][1]
                    fumble_recovery_1_player_name = check[0][1]
                    fum_start_temp = check[0][3]
                    fum_start = get_yardline(
                        fum_start_temp,
                        posteam
                    )

                    if fumble_recovery_1_team == posteam:
                        fumble_recovery_1_yards = 0
                    elif fumble_recovery_1_team == defteam:
                        fumble_recovery_1_yards = 0
                        is_fumble_lost = 1
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"`{fumble_recovery_1_team}`"
                        )

                    if "tackled by at" in desc.lower() \
                            and "fumbles, forced by. fumble recovered" \
                                in desc.lower():
                        # Edge case found in a USFL game
                        check = re.findall(
                            r"\. ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by. " +
                            r"Fumble RECOVERED by " +
                            r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Tackled by at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][4]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end

                    elif "tackled by at" in desc.lower() \
                            and "replay" not in desc.lower():
                        # Edge case found in game ID #7
                        check = re.findall(
                            r" ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                            r"([[a-zA-Z\'\.\-\s]+) Fumble RECOVERED by " +
                            r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Tackled by at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][5]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end
                    elif "tackled by at " in desc.lower():
                        pass
                    elif "tackled" in desc.lower():
                        check = re.findall(
                            r"Tackled by ([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][1]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end

                    elif "out of bounds" in desc.lower():
                        is_fumble_out_of_bounds = 1
                    elif "touchdown" in desc.lower():
                        is_return_touchdown = 1
                        return_yards = fum_start
                    elif "recovered by" in desc.lower():
                        pass
                    else:
                        raise ValueError(
                            "Unhandled play desc " +
                            f"`{desc}`"
                        )

                    del fum_start_temp

                elif "fumbles," in desc.lower() \
                        and "(aborted)" not in desc.lower()\
                        and "punt" not in desc.lower()\
                        and "muff" not in desc.lower():
                    is_fumble_forced = 1
                    is_fumble_not_forced = 0
                    is_fumble_out_of_bounds = 0
                    is_fumble_lost = 0
                    fumble_recovery_1_yards = 0

                    forced_fumble_player_1_team = ""
                    forced_fumble_player_1_player_name = ""
                    fumble_recovery_1_team = ""
                    fumble_recovery_1_player_name = ""

                    check = re.findall(
                        r" ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                        r"([[a-zA-Z\'\.\-\s]+) Fumble RECOVERED by " +
                        r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                        r"([a-zA-Z]+\s[0-9]+|" +
                        r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                        desc
                    )
                    fumbled_1_team = posteam
                    fumbled_1_player_name = check[0][0]
                    forced_fumble_player_1_team = defteam
                    forced_fumble_player_1_player_name = check[0][1]

                    fumble_recovery_1_team = check[0][2]
                    fumble_recovery_1_player_name = check[0][3]
                    fum_start_temp = check[0][4]
                    fum_start = get_yardline(
                        fum_start_temp,
                        posteam
                    )

                    if fumble_recovery_1_team == posteam:
                        fumble_recovery_1_yards = 0
                    elif fumble_recovery_1_team == defteam:
                        fumble_recovery_1_yards = 0
                        is_fumble_lost = 1
                    else:
                        raise ValueError(
                            "Unhandled fumble recovery team " +
                            f"`{fumble_recovery_1_team}`"
                        )

                    if "tackled by at" in desc.lower() \
                            and "replay" not in desc.lower():
                        # Edge case found in game ID #7
                        check = re.findall(
                            r" ([[a-zA-Z\'\.\-\s]+) FUMBLES, forced by " +
                            r"([[a-zA-Z\'\.\-\s]+) Fumble RECOVERED by " +
                            r"([a-zA-Z]+)-([[a-zA-Z\'\.\-\s]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\. " +
                            r"Tackled by at ([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][5]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end
                    elif "tackled by at " in desc.lower():
                        pass
                    elif "tackled" in desc.lower():
                        check = re.findall(
                            r"Tackled by ([[a-zA-Z\'\.\,\-\s\;]+) at " +
                            r"([a-zA-Z]+\s[0-9]+|" +
                            r"[a-zA-Z]+\s[a-zA-Z0-9]+\s[a-zA-Z]+)\.",
                            desc
                        )
                        fum_end_temp = check[0][1]
                        fum_end = get_yardline(
                            fum_end_temp,
                            posteam
                        )
                        if is_fumble_lost == 1:
                            assist_tackle_1_team = posteam
                            assist_tackle_2_team = posteam
                            solo_tackle_1_team = posteam
                            fumble_recovery_1_yards = fum_end - fum_start
                        elif is_fumble_lost == 0:
                            fumble_recovery_1_yards = fum_start - fum_end

                        del fum_end_temp
                        del fum_end

                    elif "out of bounds" in desc.lower():
                        is_fumble_out_of_bounds = 1
                    elif "touchdown" in desc.lower():
                        is_return_touchdown = 1
                        return_yards = fum_start
                    elif "recovered by" in desc.lower():
                        pass
                    else:
                        raise ValueError(
                            "Unhandled play desc " +
                            f"`{desc}`"
                        )

                    del fum_start_temp

                if "penalty" in desc.lower():
                    is_penalty = 1
                    check = re.findall(
                        r"PENALTY on ([a-zA-Z]+)-([[a-zA-Z\'\.\-\,\s]+), " +
                        r"([a-zA-Z\s]+|[a-zA-Z0-9\/\-\s\(\)]+), " +
                        r"([0-9]+) yards,.? ([a-zA-z\s]+)\.",
                        desc
                    )
                    penalty_team = check[0][0]
                    penalty_player_name = check[0][1]
                    penalty_type = check[0][2]
                    penalty_yards = check[0][3]

                    penalty_yards = int(penalty_yards)

                    if (penalty_team == defteam) \
                            and (penalty_yards >= yds_to_go) \
                            and ("accepted" in desc.lower()):
                        is_first_down = 1
                        first_down_penalty = 1

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
                        "qtr": play_quarter_num,
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
                        "third_down_failed":    third_down_failed,
                        "fourth_down_converted": fourth_down_converted,
                        "fourth_down_failed":   fourth_down_failed,
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
                        "fumble_forced":        is_fumble_forced,
                        "fumble_not_forced":    is_fumble_not_forced,
                        "fumble_out_of_bounds": is_fumble_out_of_bounds,
                        "solo_tackle": is_solo_tackle,
                        "safety": is_safety,
                        "penalty": is_penalty,
                        "tackled_for_loss": is_tackled_for_loss,
                        "fumble_lost": is_fumble_lost,
                        # "own_kickoff_recovery": is_own_kickoff_recovery,
                        # "own_kickoff_recovery_td":
                        # is_own_kickoff_recovery_td,
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
                        # "lateral_reception": lateral_reception,
                        # "lateral_rush": lateral_rush,
                        # "lateral_return": lateral_return,
                        # "lateral_recovery": lateral_recovery,
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
                        # "lateral_receiver_player_name":
                        # lateral_receiver_player_name,
                        # "lateral_receiving_yards":
                        # lateral_receiving_yards,
                        # "lateral_rusher_player_id":
                        # lateral_rusher_player_id,
                        # "lateral_rusher_player_name":
                        # lateral_rusher_player_name,
                        # "lateral_rushing_yards": lateral_rushing_yards,
                        # "lateral_sack_player_id": lateral_sack_player_id,
                        # "lateral_sack_player_name":
                        # lateral_sack_player_name,
                        # "interception_player_id": interception_player_id,
                        "interception_player_name":
                        interception_player_name,
                        # "lateral_interception_player_id":
                        # lateral_interception_player_id,
                        # "lateral_interception_player_name":
                        # lateral_interception_player_name,
                        # "punt_returner_player_id":
                        # punt_returner_player_id,
                        "punt_returner_player_name":
                        punt_returner_player_name,
                        # "lateral_punt_returner_player_id":
                        # lateral_punt_returner_player_id,
                        # "lateral_punt_returner_player_name":
                        # lateral_punt_returner_player_name,
                        "kickoff_returner_player_name":
                        kickoff_returner_player_name,
                        # "kickoff_returner_player_id":
                        # kickoff_returner_player_id,
                        # "lateral_kickoff_returner_player_id":
                        # lateral_kickoff_returner_player_id,
                        # "lateral_kickoff_returner_player_name":
                        # lateral_kickoff_returner_player_name,
                        # "punter_player_id": punter_player_id,
                        "punter_player_name": punter_player_name,
                        "kicker_player_name": kicker_player_name,
                        # "kicker_player_id": kicker_player_id,
                        # "own_kickoff_recovery_player_id":
                        # own_kickoff_recovery_player_id,
                        # "own_kickoff_recovery_player_name":
                        # own_kickoff_recovery_player_name,
                        # "blocked_player_id": blocked_player_id,
                        "blocked_player_name": blocked_player_name,
                        # "tackle_for_loss_1_player_id":
                        # tackle_for_loss_1_player_id,
                        "tackle_for_loss_1_player_name":
                        tackle_for_loss_1_player_name,
                        # "tackle_for_loss_2_player_id":
                        # tackle_for_loss_2_player_id,
                        "tackle_for_loss_2_player_name":
                        tackle_for_loss_2_player_name,
                        # "qb_hit_1_player_id": qb_hit_1_player_id,
                        # "qb_hit_1_player_name": qb_hit_1_player_name,
                        # "qb_hit_2_player_id": qb_hit_2_player_id,
                        # "qb_hit_2_player_name": qb_hit_2_player_name,
                        "forced_fumble_player_1_team":
                        forced_fumble_player_1_team,
                        # "forced_fumble_player_1_player_id":
                        # forced_fumble_player_1_player_id,
                        "forced_fumble_player_1_player_name":
                        forced_fumble_player_1_player_name,
                        # "forced_fumble_player_2_team":
                        # forced_fumble_player_2_team,
                        # "forced_fumble_player_2_player_id":
                        # forced_fumble_player_2_player_id,
                        # "forced_fumble_player_2_player_name":
                        # forced_fumble_player_2_player_name,
                        "solo_tackle_1_team": solo_tackle_1_team,
                        "solo_tackle_2_team": solo_tackle_2_team,
                        # "solo_tackle_1_player_id":
                        # solo_tackle_1_player_id,
                        # "solo_tackle_2_player_id":
                        # solo_tackle_2_player_id,
                        "solo_tackle_1_player_name":
                        solo_tackle_1_player_name,
                        "solo_tackle_2_player_name":
                        solo_tackle_2_player_name,
                        # "assist_tackle_1_player_id":
                        # assist_tackle_1_player_id,
                        "assist_tackle_1_player_name":
                        assist_tackle_1_player_name,
                        "assist_tackle_1_team": assist_tackle_1_team,
                        # "assist_tackle_2_player_id":
                        # assist_tackle_2_player_id,
                        "assist_tackle_2_player_name":
                        assist_tackle_2_player_name,
                        "assist_tackle_2_team": assist_tackle_2_team,
                        # "assist_tackle_3_player_id":
                        # assist_tackle_3_player_id,
                        "assist_tackle_3_player_name":
                        assist_tackle_3_player_name,
                        "assist_tackle_3_team": assist_tackle_3_team,
                        # "assist_tackle_4_player_id":
                        # assist_tackle_4_player_id,
                        "assist_tackle_4_player_name":
                        assist_tackle_4_player_name,
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
                        "pass_defense_1_player_name":
                        pass_defense_1_player_name,
                        # "pass_defense_2_player_id":
                        # pass_defense_2_player_id,
                        "pass_defense_2_player_name":
                        pass_defense_2_player_name,
                        "fumbled_1_team": fumbled_1_team,
                        # "fumbled_1_player_id":
                        # fumbled_1_player_id,
                        "fumbled_1_player_name": fumbled_1_player_name,
                        # "fumbled_2_player_id":
                        # fumbled_2_player_id,
                        # "fumbled_2_player_name": fumbled_2_player_name,
                        # "fumbled_2_team": fumbled_2_team,
                        "fumble_recovery_1_team": fumble_recovery_1_team,
                        "fumble_recovery_1_yards": fumble_recovery_1_yards,
                        # "fumble_recovery_1_player_id":
                        # fumble_recovery_1_player_id,
                        "fumble_recovery_1_player_name":
                        fumble_recovery_1_player_name,
                        # "fumble_recovery_2_team": fumble_recovery_2_team,
                        # "fumble_recovery_2_yards":
                        # fumble_recovery_2_yards,
                        # "fumble_recovery_2_player_id":
                        # fumble_recovery_2_player_id,
                        # "fumble_recovery_2_player_name":
                        # fumble_recovery_2_player_name,
                        # "sack_player_id": sack_player_id,
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
                        "replay_or_challenge_result":
                        replay_or_challenge_result,
                        "penalty_type": penalty_type,
                        # "defensive_two_point_attempt":
                        # defensive_two_point_attempt,
                        # "defensive_two_point_conv":
                        # defensive_two_point_conv,
                        # "defensive_extra_point_attempt":
                        # defensive_extra_point_attempt,
                        # "defensive_extra_point_conv":
                        # defensive_extra_point_conv,
                        # "safety_player_id": safety_player_id,
                        # "safety_player_name": safety_player_name,
                        "season": season,
                        # "series": series_id,
                        # "series_success": series_success,
                        # "series_result": series_result,
                        # "order_sequence": order_sequence,
                        "start_time": None,
                        "time_of_day": None,
                        "stadium": stadium,
                        "play_clock": 0,
                        "play_deleted": 0,
                        # "play_type_nfl": play_type_nfl,
                        "special_teams_play": is_special_teams_play,
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
                        # "drive_yards_penalized": drive_yards_penalized,
                        # "drive_start_transition": drive_start_transition,
                        # "drive_end_transition": drive_end_transition,
                        # "drive_game_clock_start": drive_game_clock_start,
                        # "drive_game_clock_end": drive_game_clock_end,
                        # "drive_start_yard_line": drive_start_yard_line,
                        # "drive_end_yard_line": drive_end_yard_line,
                        # "drive_play_id_started": drive_play_id_started,
                        # "drive_play_id_ended": drive_play_id_ended,
                        "home_score": total_home_score,
                        "away_score": total_away_score,
                        "location": "Home",  # either "Home" or "Neutral"
                        # "result": 0,
                        # "total": betting_total,
                        # "spread_line": spread_line,
                        # "total_line": total_line,
                        # "div_game": is_divisional_game,
                        # "roof": roof,
                        # "surface": playing_surface,
                        # "temp": game_temp,
                        # "wind": game_wind,
                        # "home_coach": home_coach,
                        # "away_coach": away_coach,
                        "stadium_id": None,
                        "game_stadium": stadium,
                        "aborted_play": is_aborted_play,
                        # "success": is_successful_play,
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
                plays_arr.append(temp_df)

                del temp_df

        play_id += 1

    plays_df = pd.concat(plays_arr, ignore_index=True)

    plays_df["away_score"] = total_away_score
    plays_df["home_score"] = total_home_score
    plays_df["result"] = total_home_score - total_away_score
    plays_df["total"] = total_home_score + total_away_score

    return plays_df


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


if __name__ == "__main__":
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
