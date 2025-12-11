# URMA Hawaii Hourly Meteorology Assets

URMA Hawaii hourly meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/urma/hawaii/hourly

Timestep: hourly

Image name format: YYYYMMDDHH

### Bands

| Band      | Description                         | Units   |
|-----------|-------------------------------------|---------|
| TMP       | Air temperature (@ 2m)              | C       |
| DPT       | Dewpoint temperature (@ 2m)         | C       |
| SPFH      | Specific humidity                   | kg kg-1 |
| PRES      | Surface pressure                    | Pa      |
| WDIR      | Wind direction (from which blowing) | deg     |
| WIND      | Wind speed (@ 10m)                  | m s-1   |
| TCDC      | Total cloud cover                   | %       |
| SRAD_TCDC | Solar radiation (from TCDC)         | W m-2   |
| ETO       | ASCE reference ET (grass)           | mm      |
| ETR       | ASCE reference ET (alfalfa)         | mm      |

Other variables that are not currently included in the assets:

| Band | Description                         | Units |
|------|-------------------------------------|-------|
| HGT  | Model terrain elevation             | m     |
| UGRD | U-component of wind (@ 10m)         | m s-1 |
| VGRD | V-component of wind (@ 10m)         | m s-1 |
| WDIR | Wind direction (from which blowing) | deg   |
| GUST | Wind gust (@ 10m)                   | m s-1 |
| VIS  | Visibility                          | m     |

### Solar Radiation Source

Solar radiation data is computed from the total cloud cover following ?.

## Availability

URMA Hawaii is currently available for 2020-01-01 to present in GEE, but data back to 2016-08-01 is available and could be ingested.

### References



## Cloud Functions

The asset ingest is currently being managed using Google Cloud Functions:

https://console.cloud.google.com/functions/details/us-central1/urma-hi-meteorology-hourly?project=openet

The cloud function is called by the Cloud Scheduler:

https://console.cloud.google.com/cloudscheduler?project=openet

### Deploying the cloud function

```
gcloud functions deploy urma-hi-meteorology-hourly --project openet --no-gen2 --runtime python311 --entry-point cron_scheduler --trigger-http --memory 512 --timeout 540 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1 --allow-unauthenticated --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.
```
gcloud functions call urma-hi-meteorology-hourly --project openet --data '{"start":"2021-07-01","end":"2021-08-01"}'
```

On Windows, the data parameter is a little different
```
gcloud functions call urma-hi-meteorology-hourly --project openet --data "{\"date\":\"2020-09-01\"}\"
```

If no date arguments are passed it will check the last 90 days for missing assets.
```
gcloud functions call urma-hi-meteorology-hourly --project openet
```

### Scheduling the job

The "update" parameter will need to be changed "create" the first time the job is scheduled.
```
gcloud scheduler jobs update http urma-hi-meteorology-hourly --schedule "7 4 * * *" --uri "https://us-central1-openet.cloudfunctions.net/urma-hi-meteorology-hourly" --description "URMA Hawaii Hourly Meteorology" --http-method POST --time-zone "Etc/UTC" --project openet --location us-central1 --max-retry-attempts 2 --attempt-deadline 480s --min-backoff=20s
```
