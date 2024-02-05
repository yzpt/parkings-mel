# Parkings MEL
# https://metropole-europeenne-de-lille.opendatasoft.com/explore/dataset/disponibilite-parkings/information/?flg=fr

# API
# https://metropole-europeenne-de-lille.opendatasoft.com/api/explore/v2.1/catalog/datasets/disponibilite-parkings/records?limit=100

# 1. GCP project configuration

PROJECT_ID="parkings-mel"
REGION="europe-west2"
ZONE="europe-west2-a"
SERVICE_ACCOUNT="admin-$PROJECT_ID"

# create a new project
gcloud projects create $PROJECT_ID

# set the project
gcloud config set project $PROJECT_ID

# service account
gcloud iam service-accounts create $SERVICE_ACCOUNT

# grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" --role="roles/storage.admin"

