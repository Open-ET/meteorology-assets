# URMA Hawaii Daily Meteorology Assets

URMA Hawaii daily meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/urma/hawaii/daily

Timestep: daily

Image name format: YYYYMMDD

### Bands

| Band      | Description                          | Units   |
|-----------|--------------------------------------|---------|
| TMAX      | Daily maximum air temperature (@ 2m) | C       |
| TMIN      | Daily minimum air temperature (@ 2m) | C       |
| TAVG      | Daily mean air temperature (@ 2m)    | C       |
| DPT       | Dewpoint temperature (@ 2m)          | C       |
| SPFH      | Specific humidity                    | kg kg-1 |
| PRES      | Surface level pressure               | Pa      |
| WIND      | Wind speed (@ 10m)                   | m s-1   |
| TCDC      | Total cloud cover                    | %       |
| SRAD_TCDC | Surface downward shortwave radiation | MJ m-2  |
| ETO       | ASCE reference ET (grass)            | mm      |
| ETR       | ASCE reference ET (alfalfa)          | mm      |

### Solar Radiation Source

Solar radiation data is computed from the total cloud cover following ?.

### Daily Aggregation

The daily aggregation starts (and ends) at 10 UTC to better represent the day for Hawaii.  The start date is lagged by one day because of the 10 UTC start time. 

Reference ET was computed at the hourly timestep and then summed to the day.

## Availability

URMA Hawaii is currently available for 2020-01-01 to present in GEE, but data back to 2016-08-01 is available and could be ingested.

### References



## Cloud Functions

The asset ingest is currently being managed using Google Cloud Functions:

https://console.cloud.google.com/functions/details/us-central1/urma-hi-meteorology-daily?project=openet

The cloud function is called by the Cloud Scheduler:

https://console.cloud.google.com/cloudscheduler?project=openet

### Deploying the cloud function

```
gcloud functions deploy urma-hi-meteorology-daily --project openet --no-gen2 --runtime python311 --entry-point cron_scheduler --trigger-http --memory 512 --timeout 540 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1 --allow-unauthenticated --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.
```
gcloud functions call urma-hi-meteorology-daily --project openet --data '{"start":"2021-07-01","end":"2021-08-01","region":"hawaii"}'
```

On Windows, the data parameter is a little different
```
gcloud functions call urma-hi-meteorology-daily --project openet --data "{\"date\":\"2020-09-01\"}\"
```

If no date arguments are passed it will check the last 90 days for missing assets.
```
gcloud functions call urma-hi-meteorology-daily --project openet --data '{"region":"hawaii"}'
```

### Scheduling the job

The "update" parameter will need to be changed "create" the first time the job is scheduled.
```
gcloud scheduler jobs update http urma-hi-meteorology-daily --schedule "7 4 * * *" --uri "https://us-central1-openet.cloudfunctions.net/urma-hi-meteorology-daily" --description "URMA Daily Meteorology" --http-method POST --time-zone "Etc/UTC" --project openet --location us-central1 --max-retry-attempts 2 --attempt-deadline 480s --min-backoff=20s
```
