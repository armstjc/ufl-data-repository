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

| Column Name       | Description                                                                         |
| ----------------- | ----------------------------------------------------------------------------------- |
| season            | The season these standings come from.                                               |
| week              | The week these standings come from.                                                 |
| team_id           | The FOX Sports Team ID for this UFL team.                                           |
| team_analytics_id | The FOX Sports Team Analytics ID for this UFL team.                                 |
| team_name         | The full name of this UFL team.                                                     |
| rank              | The ranking of this team in either its conference, or in the UFL itself.            |
| G                 | The total number of games this UFL team has played in.                              |
| W                 | The number of wins this UFL team has to this point in the season.                   |
| L                 | The number of losses this UFL team has to this point in the season.                 |
| W%                | The win percentage of this UFL team to this point in the season.                    |
| PF                | The total number of points scored by this UFL team up to this point in the season.  |
| PA                | The total number of points allowed by this UFL team up to this point in the season. |
| home_W            | The number of home wins this UFL team has to this point in the season.              |
| home_L            | The number of home losses this UFL team has to this point in the season.            |
| away_W            | The number of away wins this UFL team has to this point in the season.              |
| away_L            | The number of away losses this UFL team has to this point in the season.            |
| div_W             | The number of divisional wins this UFL team has to this point in the season.        |
| div_L             | The number of divisional losses this UFL team has to this point in the season.      |
| streak            | The winning or losing streak this UFL team was on at this point in the season.      |
| team_logo         | A URL that points to this UFL team's official logo.                                 |
