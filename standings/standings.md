# Standings

The standings data for the UFL is split into two releases:

- "UFL Standings": The most up to date standings for the league for every season of data available.
- "UFL Weekly Standings": The standings for every week of data in the UFL.

## Data Endpoints

[UFL Standings](https://github.com/armstjc/ufl-data-repository/releases/tag/ufl-standings)

[UFL Weekly Standings](https://github.com/armstjc/ufl-data-repository/releases/tag/ufl-weekly-standings)

## File format

File formats available are `.csv`, `.parquet`, and the raw `.json`.

For the normal standings data, the filename follows the following naming convention:

`{season}_ufl_standings.csv`

For weekly standings flies, the filename follows the following naming convention:

`{season}-{week}_ufl_standings.csv`

_NOTE: `{week}` will have a leading zero if `{week} < 10`. So if you want the UFL standings for week 5 of the 2024 season, the filename will look like this:_

`2024-04_ufl_standings.csv`

## Columns

| Column Name          | description                                                                                                                                        |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| season               | The season these standings come from.                                                                                                              |
| week                 | The week these standings come from.                                                                                                                |
| sr_id                | ???                                                                                                                                                |
| team_uid             | A Unique Identifier (UID) for this team.                                                                                                           |
| team_abbreviation    | The abbreviation for this UFL team.                                                                                                                |
| statbroadcast_id     | The StatBroadcast ID for this UFL team.                                                                                                            |
| team_location        | The city this team plays in, or at the very least, intended to play in.                                                                            |
| team_nickname        | The nickname of this team                                                                                                                          |
| conference_name      | The name of the conference this team plays in.                                                                                                     |
| conference_abv       | The abbreviation for the conference this UFL team plays in.                                                                                        |
| conference_rank      | The rank of this UFL team in their conference.                                                                                                     |
| games_played         | The number of games this UFL team has played in to this point in the season.                                                                       |
| wins                 | The number of wins this UFL team has to this point in the season.                                                                                  |
| losses               | The number of losses this UFL team has to this point in the season.                                                                                |
| ties                 | The number of ties this UFL team has to this point in the season.                                                                                  |
| win_pct              | The win percentage of this UFL team to this point in the season.                                                                                   |
| strength_of_schedule | The strength of schedule of this UFL team to this point in the season.                                                                             |
| strength_of_victory  | ???                                                                                                                                                |
| access_time          | The date and time the standings file was last updated, formatted in accordance to the [ISO 8061 standard](https://en.wikipedia.org/wiki/ISO_8601). |
