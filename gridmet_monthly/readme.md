# GRIDMET Monthly Meteorology Assets

GRIDMET monthly meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/gridmet/monthly

Timestep: monthly

Image name format: YYYYMM

### Bands

| Band | Description                 | Units |
|------|-----------------------------|-------|
| pr   | Precipitation               | mm    |
| eto  | ASCE reference ET (grass)   | mm    |
| etr  | ASCE reference ET (alfalfa) | mm    |

## Cloud Functions

The asset ingest is currently being managed using Google Cloud Functions

https://console.cloud.google.com/functions/details/us-central1/gridmet-meteorology-monthly?project=openet

The cloud function is called by the Cloud Scheduler:
https://console.cloud.google.com/cloudscheduler?project=openet

### Set the project ID

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.
```
gcloud config set project openet
```

### Deploying the cloud function

```
gcloud functions deploy gridmet-meteorology-monthly --project openet --no-gen2 --runtime python311 --region us-central1 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 512 --timeout 240 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1 --set-env-vars FUNCTION_REGION=us-central1
```

### Calling the cloud function

```
gcloud functions call gridmet-meteorology-monthly --project openet --data '{"start":"2021-01-01", "end":"2021-05-31"}'
```

```
gcloud functions call gridmet-meteorology-monthly --project openet
```

### Scheduling the job

```
gcloud scheduler jobs update http gridmet-meteorology-monthly-v1 --schedule "12 6 5 * *" --uri "https://us-central1-openet.cloudfunctions.net/gridmet-meteorology-monthly-v1" --description "Update Monthly GRIDMET Meteorology (PPT)" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 3 --attempt-deadline 300s --min-backoff=20s
```

--oidc-service-account-email openet-assets-queue@openet.iam.gserviceaccount.com --oidc-token-audience "https://us-central1-openet.cloudfunctions.net/gridmet-meteorology-monthly" 