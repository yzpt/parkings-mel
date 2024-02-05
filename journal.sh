# Parkings MEL
# https://metropole-europeenne-de-lille.opendatasoft.com/explore/dataset/disponibilite-parkings/information/?flg=fr

# API
# https://metropole-europeenne-de-lille.opendatasoft.com/api/explore/v2.1/catalog/datasets/disponibilite-parkings/records?limit=100

# variables
PROJECT_ID="parkings-mel"
REGION="europe-west2"
ZONE="europe-west2-a"
SERVICE_ACCOUNT="admin-$PROJECT_ID"
SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com
DATA_BUCKET="gs://parkings-mel-data"
FUNCTION_BUCKET="gs://parkings-mel-function"
DATASET_NAME="parkings_mel"

# git
git init
git add .
git commit -m "first commit"
git remote add origin https://github.com/yzpt/parkings-mel.git
git push -u origin main

# venv
python3 -m venv venv
source venv/bin/activate
pip install google-cloud-bigquery 
pip install google-cloud-storage

# 1. GCP project configuration

# create a new project
gcloud projects create $PROJECT_ID

# set the project
gcloud config set project $PROJECT_ID

# service account
gcloud iam service-accounts create $SERVICE_ACCOUNT

# grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/storage.admin"

# create key for the service account
gcloud iam service-accounts keys create key.json --iam-account=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com


# list billing accounts
gcloud billing accounts list
# Linking billing accout to the project
gcloud billing projects link $PROJECT_ID --billing-account=016507-8E76F6-937B68


# Data collection

# Enabling APIs: Build, Functions, Pub/Sub, Scheduler
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable cloudscheduler.googleapis.com

# Creating GCS bucket for data collection
gcloud storage buckets create $DATA_BUCKET
# Creating GCS bucket for function storage
gcloud storage buckets create $FUNCTION_BUCKET

# BigQuery dataset
bq mk $DATASET_NAME
#delete dataset
bq rm -r -f $DATASET_NAME

# --> nb_bigquery.ipynb

