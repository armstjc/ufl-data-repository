# Schedule

The schedule data for the UFL is stored in a single release, named "UFL Schedule"

## Data Endpoint

[UFL Schedule](https://github.com/armstjc/ufl-data-repository/releases/tag/ufl-schedule)

## File format

File formats available are `.csv`, `.parquet`, and the raw `.json`.

For UFL schedule data for a given season, the filename follows the following naming convention:

`{season}_ufl_schedule.csv`

## Columns

| Column Name              | Description                                                                                                 |
| ------------------------ | ----------------------------------------------------------------------------------------------------------- |
| season                   | The UFL season this game belong to.                                                                         |
| season_type              | Denotes what part of the UFL season this game is part of.                                                   |
| week_id                  | An ID within the FOX Sports API that identifies which week this game takes place in.                        |
| week_title               | A formatted string describing which week this game takes place in.                                          |
| ufl_game_id              | The FOX Sports ID for this game in the UFL                                                                  |
| game_date                | Either the date this UFL game took place in, or the date this UFL game is scheduled to take place in.       |
| away_team_id             | The FOX Sports ID for the away team in this game.                                                           |
| away_team_analytics_name | The FOX Sports analytics ID for the away team in this game.                                                 |
| away_team_name           | The full name of the away team in this UFL game.                                                            |
| home_team_id             | The FOX Sports ID for the home team in this game.                                                           |
| home_team_analytics_name | The FOX Sports analytics ID for the home team in this game.                                                 |
| home_team_name           | The full name of the home team in this UFL game.                                                            |
| away_score               | The current or final score of the away team in this UFL game.                                               |
| home_score               | The current or final score of the home team in this UFL game.                                               |
| stadium                  | The stadium this UFL game took place in.                                                                    |
| location                 | The city this UFL game took place in.                                                                       |
| fox_bet_odds             | If available, the values in this column are the FOX BET odds for this UFL game.                             |
| scheduled_date           | The date and time this game is scheduled to be played at.                                                   |
| broadcast_network        | The broadcast network this game is scheduled to be shown at.                                                |
| last_updated             | The [ISO 8061](https://en.wikipedia.org/wiki/ISO_8601) date and time of when these rosters were downloaded. |
