# Rosters

The rosters data for the UFL is split into two releases:

- "UFL Rosters": The most up to date rosters for the league for every season of data available.
- "UFL Weekly Rosters": The roster for every team, in every week of data in the UFL.

## Data Endpoints

[UFL Rosters](https://github.com/armstjc/ufl-data-repository/releases/tag/ufl-rosters)

[UFL Weekly Rosters](https://github.com/armstjc/ufl-data-repository/releases/tag/ufl-weekly-rosters)

## File format

File formats available are `.csv`, `.parquet`, and the raw `.json`.

For the normal rosters data, the filename follows the following naming convention:

`{season}_ufl_rosters.csv`

For weekly rosters flies, the filename follows the following naming convention:

`{season}-{week}_ufl_rosters.csv`

_NOTE: `{week}` will have a leading zero if `{week} < 10`. So if you want the UFL rosters for week 5 of the 2024 season, the filename will look like this:_

`2024-04_ufl_rosters.csv`

## Columns

