# Baseball Zavant

This repo is an ongoing baseball analytics project inspired by [MLB's Baseball Savant](https://baseballsavant.mlb.com/). All products are built from [live game data](https://github.com/zpgallegos/zavant/blob/master/docs/example-game-raw.json), queried and ETL'd from scratch from the [MLB Stats API](https://statsapi.mlb.com). (See the [copyright notice](#copyright-notice).)

## Transformation and App Update Pipeline

The data pipeline is facilitated via AWS services, orchestrated via [this state machine](https://github.com/zpgallegos/zavant/blob/master/landing/step-functions/zavant-update-pipeline-sanitized.json) in Step Functions:

![Pipeline](landing/step-functions/pipeline.png)

Steps:

1. **DownloadNewGames**: Download new game files from the API - [Lambda](https://github.com/zpgallegos/zavant/blob/master/landing/lambda/zavant-download-games/function/lambda_function.py)

    - Raw game files that look like [this]([live game data](https://github.com/zpgallegos/zavant/blob/master/docs/example-game-raw.json)) are saved to S3

2. **CheckEventTriggerSuccessful**: Monitor processed for new files - [Lambda](https://github.com/zpgallegos/zavant/blob/master/landing/lambda/zavant-monitor-for-flattened/function/lambda_function.py)

    - The raw bucket has an S3 event trigger that's tied to [another function](https://github.com/zpgallegos/zavant/tree/master/landing/lambda/zavant-process-raw-game) to flatten out the game object and extract relevant pieces to their own separate buckets
    - This function checks for the presence of all expected files before proceeding, triggering the next step when they're all there, or a wait step otherwise (max 3 retries)

3. **ConvertToParquet**: Apply [predefined schemas](https://github.com/zpgallegos/zavant/blob/master/landing/glue/schemas) to the JSON data and convert to Parquet - [PySpark executed via Glue](https://github.com/zpgallegos/zavant/blob/master/landing/glue/statsapi_convert_json_to_parquet.py)
    - The data is partitioned by season to match query intent
    - Crawlers populate the Glue Data Catalog with the data lake tables loaded by this step
    - These tables are used as the source tables for the dbt project

![Data Model](docs/zavant_datamart.png)

4. **UpdateDBTModels**: Update the [dbt models](https://github.com/zpgallegos/zavant/tree/master/dbt/zavant_mlb_analytics) that serve as the end products for the application

    - Run as [a container](https://github.com/zpgallegos/zavant/blob/master/dbt/Dockerfile) in an ECS Fargate task

5. **RebuildStaticSite**: Create a new build of the [React front end](https://github.com/zpgallegos/zavant/tree/master/app) and deploy as a static site

## Copyright Notice

This repo and its author are not affiliated with MLB or any MLB team. The code in this repo interfaces with MLB's Stats API. Use of MLB data is subject to the notice posted [here](http://gdx.mlb.com/components/copyright.txt).
