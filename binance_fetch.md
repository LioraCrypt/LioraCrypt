# executer dans cloud shell


### TEST MANUEL
# activation des services
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com

# Job, image docker CURL publique, datacenter Paris, deux bougies, échec du job en cas de code 4XX/5XX sans retry
gcloud run jobs create binance-probe \
  --image=docker.io/curlimages/curl:8.21.0 \
  --region=europe-west9 \
  --command=curl \
  --args="--silent,--show-error,--include,--fail-with-body,--max-time,20,https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=2" \
  --tasks=1 \
  --max-retries=0 \
  --task-timeout=60s \
  --memory=512Mi \
  --cpu=1

# Execution
gcloud run jobs execute binance-probe \
  --region=europe-west9 \
  --wait

### AUTOMATISATION
# Autorisation cloud scheduler execution job
PROJECT_ID=$(gcloud config get-value project)

gcloud iam service-accounts create binance-scheduler \
  --display-name="Scheduler Binance Test"

# Limitation du scheduler à ce job uniquement
gcloud run jobs add-iam-policy-binding binance-probe \
  --region=europe-west9 \
  --member="serviceAccount:binance-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# Paramétrer le scheduler manuellement ou faire:
PROJECT_ID=$(gcloud config get-value project)

gcloud scheduler jobs create http binance-hourly-test \
  --location=europe-west1 \
  --schedule="0 * * * *" \
  --time-zone="Europe/Paris" \
  --uri="https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/europe-west9/jobs/binance-probe:run" \
  --http-method=POST \
  --oauth-service-account-email="binance-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"