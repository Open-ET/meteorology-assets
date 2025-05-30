# ERA5-Land Monthly Meteorology Asset

ERA5-Land monthly meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/era5land/na/monthly
Collection ID: projects/openet/assets/meteorology/era5land/monthly

Timestep: monthly

Image name format: YYYYMM

### Bands

| Band                                | Description                          | Units |
|-------------------------------------|--------------------------------------|-------|
| total_precipitation                 | Total precipitation                  | m     |
| eto_asce                            | ASCE reference ET (grass)            | mm    |
| etr_asce                            | ASCE reference ET (alfalfa)          | mm    |

### References

https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Parameterlistings

## Cloud Functions

The asset ingest is currently being managed using Google Cloud Functions

https://console.cloud.google.com/functions/details/us-central1/era5land-meteorology-monthly?project=openet

The cloud function is called by the Cloud Scheduler:
https://console.cloud.google.com/cloudscheduler?project=openet

### Set the project ID

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.
```
gcloud config set project openet
```

### Deploying the cloud function

```
gcloud functions deploy era5land-meteorology-monthly --project openet--no-gen2 --runtime python311 --entry-point cron_scheduler --trigger-http --memory 512 --timeout 540 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1 --allow-unauthenticated --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

The functions can be called by passing JSON data to the function.
```
gcloud functions call era5land-meteorology-monthly --project openet --data '{"start":"2021-07-01","end":"2021-08-01","region":"na"}'
```

On Windows, the data parameter is a little different
```
gcloud functions call era5land-meteorology-monthly --project openet --data "{\"date\":\"2020-09-01\"}\"
```

If no date arguments are passed it will check the last 90 days for missing assets.
```
gcloud functions call era5land-meteorology-monthly --project openet --data '{"region":"na"}'
```

### Scheduling the job

The "update" parameter will need to be changed "create" the first time the job is scheduled.
```
gcloud scheduler jobs update http era5land-meteorology-monthly --schedule "20 4 5 * *" --uri "https://us-central1-openet.cloudfunctions.net/era5land-meteorology-monthly?region=na" --description "ERA5-Land Monthly Meteorology" --http-method POST --time-zone "Etc/UTC" --project openet --location us-central1 --max-retry-attempts 2 --attempt-deadline 480s --min-backoff=20s
```
