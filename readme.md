# Baseball Zavant

A clone of some statistical tables presented on [MLB's Baseball Savant](https://baseballsavant.mlb.com/). All data is queried from the [MLB Stats API](https://statsapi.mlb.com) and ETL'd from scratch using Python and PySpark. Output player pages (dev in progress) are hosted on AWS: see [Mookie's page](http://zavant.zgallegos.com/players/605141/), for example.

The pipeline is as follows:
1. A [Lambda function](https://github.com/zpgallegos/zavant/blob/master/aws/zavant-download-games/lambda_function.py) runs nightly, checking the day's scheduled games against an S3 bucket. Any new games are downloaded and landed in raw form to the bucket.
2. A [second Lambda function](https://github.com/zpgallegos/zavant/blob/master/aws/zavant-process-raw-game/lambda_function.py) running on an event trigger from the raw bucket picks up the file and preprocesses it into JSON data that will occupy seven tables:
    * zavant-game-data: info about the game itself (start time, status, field, etc.)
    * zavant-game-teams: info abou◊t the teams in the game
    * zavant-game-players: info about the players in the game
    * zavant-game-boxscore: stats used to populate the boxscore of the game
    * zavant-play-info: high-level play types and outcomes (e.g., single, walk, strikeout, etc.)
    * zavant-play-events: events that constitute the plays. most stats are calculated with these
    * zavant-play-runners: runner movement in the plays  

    Information for each table is parsed out of the heavily nested JSON data into a flat structure and saved to a dedicated bucket for that data type.  

3. (Runs once) AWS Glue crawlers are configured to crawl the buckets and populate tables for the processed data in the Glue Data Catalog.
4. Glue PySpark ETL jobs run to produce the final data products for reporting. These are run once to produce the figures for historical seasons, then nightly in-season to update stats for the current season.
    * [MLB Standard Batting](https://www.baseball-reference.com/leagues/majors/2022-standard-batting.shtml)
5. A [simple Nuxt app](https://github.com/zpgallegos/zavant/tree/master/web) is used to create files for the root site and player leaf pages to be hosted as static sites on S3. See [Mookie's page](http://zavant.zgallegos.com/players/605141/), for example.

## Copyright Notice

This repo and its author are not affiliated with MLB or any MLB team. The code in this repo interfaces with MLB's Stats API. Use of MLB data is subject to the notice posted [here](http://gdx.mlb.com/components/copyright.txt).
