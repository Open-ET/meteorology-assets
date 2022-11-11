# GRIDMET Monthly Meteorlogy Assets

### Set the project ID

Before deploying or calling the cloud functions, the "project" can be set once with the following call, or passed to each gcloud call.
```
gcloud config set project openet
```

### Deploying the cloud function

```
gcloud functions deploy gridmet-meteorology-monthly --project openet --runtime python37 --region us-central1 --entry-point cron_scheduler --trigger-http --allow-unauthenticated --memory 512 --timeout 240 --service-account="openet-assets-queue@openet.iam.gserviceaccount.com" --max-instances 1
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
gcloud scheduler jobs update http gridmet-meteorology-monthly --schedule "12 6 5 * *" --uri "https://us-central1-openet.cloudfunctions.net/gridmet-meteorology-monthly" --description "Update Monthly GRIDMET Meteorology (PPT)" --http-method POST --time-zone "UTC" --project openet --location us-central1 --max-retry-attempts 3 --attempt-deadline 300s --min-backoff=20s
```


--oidc-service-account-email openet-assets-queue@openet.iam.gserviceaccount.com --oidc-token-audience "https://us-central1-openet.cloudfunctions.net/gridmet-meteorology-monthly" 