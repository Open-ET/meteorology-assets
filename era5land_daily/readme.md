# ERA5-Land Daily Meteorology Assets

ERA5-Land daily meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/era5land/na/daily
Collection ID: projects/openet/assets/meteorology/era5land/sa/daily
Collection ID: projects/openet/assets/meteorology/era5land/eu/daily
Collection ID: projects/openet/assets/meteorology/era5land/daily

Timestep: daily

Image name format: YYYYMMDD

### Bands

| Band                                | Description                          | Units |
|-------------------------------------|--------------------------------------|-------|
| temperature_2m_max                  | Daily maximum air temperature (@ 2m) | K     |
| temperature_2m_min                  | Daily minimum air temperature (@ 2m) | K     |
| temperature_2m_mean                 | Daily mean air temperature (@ 2m)    | K     |
| dewpoint_temperature_2m             | Dewpoint temperature (@ 2m)          | K     |
| wind_10m                            | Wind speed (@ 10m)                   | m s-1 |
| surface_solar_radiation_downwards   | Rs in                                | J m-2 |
| total_precipitation                 | Total precipitation                  | m     |
| eto_asce                            | ASCE reference ET (grass)            | mm    |
| etr_asce                            | ASCE reference ET (alfalfa)          | mm    |

### Daily Aggregation

The daily aggregations have different start (and end) times depending on the region.
 - North America ("na"): 6 UTC
 - South America ("sa"): 3 UTC
 - European Union ("eu"): 0 UTC
 - Global ("global"): 0 UTC

Reference ET was computed from the daily aggregated values, not as a sum of the hourly.

### References

https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Parameterlistings

## Cloud Functions

The asset ingest is currently being managed using Google Cloud Functions

https://console.cloud.google.com/functions/details/us-central1/era5land-meteorology-daily?project=openet

The cloud function is called by the Cloud Scheduler

https://console.cloud.google.com/cloudscheduler?project=openet

### Set the project ID

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.

```
gcloud config set project openet
```

### Deploying the cloud function

```
gcloud functions deploy era5land-meteorology-daily --project openet --no-gen2 --runtime python311 --entry-point cron_scheduler --trigger-http --memory 512 --timeout 540 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1 --allow-unauthenticated --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.

```
gcloud functions call era5land-meteorology-daily --project openet --data '{"start":"2021-07-01","end":"2021-08-01","region":"na"}'
```

On Windows, the data parameter is a little different.

```
gcloud functions call era5land-meteorology-daily --project openet --data "{\"date\":\"2020-09-01\"}\"
```

If no date arguments are passed it will check the last 90 days for missing assets.

```
gcloud functions call era5land-meteorology-daily --project openet --data '{"region":"na"}'
```

### Scheduling the job

The "update" parameter will need to be changed "create" the first time the job is scheduled.

```
gcloud scheduler jobs create http era5land-meteorology-daily-na --schedule "7 4 * * *" --uri "https://us-central1-openet.cloudfunctions.net/era5land-meteorology-daily?region=na&refet_timestep=daily&fill_edge_cells=0" --description "ERA5-Land Daily Meteorology (North America)" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 2 --attempt-deadline 480s --min-backoff=20s
```

```
gcloud scheduler jobs create http era5land-meteorology-daily-eu --schedule "17 4 * * *" --uri "https://us-central1-openet.cloudfunctions.net/era5land-meteorology-daily?region=eu&refet_timestep=hourly&fill_edge_cells=2" --description "ERA5-Land Daily Meteorology (European Union)" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 2 --attempt-deadline 480s --min-backoff=20s
```
