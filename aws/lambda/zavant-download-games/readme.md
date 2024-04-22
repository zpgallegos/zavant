# zavant-download-games

Lambda function to download raw game files from the [MLB Stats API](https://statsapi.mlb.com)

Environmental variables:
* OUT_BUCKET: S3 bucket to store the downloaded files
* CURRENT_SEASON: current season to download files for
    * the current season is always downloaded on each run
    * the event payload can specify additional seasons to download:

Event:
```json
{
  "past_seasons": [2018, 2019]
}
```


