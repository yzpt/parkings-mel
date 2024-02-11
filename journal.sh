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

# venv_fct
python3 -m venv venv
source venv/bin/activate
pip install google-cloud-bigquery 
pip install google-cloud-storage

# venv_dev
python3 -m venv venv_dev
source venv_dev/bin/activate
pip install ipykernel
pip install google-cloud-bigquery 
pip install google-cloud-storage
pip install pandas
pip install pyarrow
pip install pandas-gbq
pip install tqdm
pip install matplotlib
pip install jupyter

# convert notebook to markdown
jupyter nbconvert --to markdown pandas_bigquery_select.ipynb --output-dir=docs

# === 1. GCP project configuration ========================================================================

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
gcloud billing projects link $PROJECT_ID --billing-account=******-******-******


# === 2. Data collection ========================================================================

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

# *** --> nb_bigquery.ipynb ***

# === 3. Cloud Function ========================================================================
# --> fct_extract/
mkdir fct_extract
touch fct_extract/main.py
code fct_extract/main.py
cp key.json fct_extract/key.json

# fct venv
python3 -m venv venv_fct_extract
source venv_fct_extract/bin/activate
pip install google-cloud-bigquery
pip freeze > fct_extract/requirements.txt

python3 fct_extract/main.py
# --> test ok

# zip the fct_extract folder
cd fct_extract
zip -r fct_extract.zip .
cd ..
mv fct_extract/fct_extract.zip .

# upload the zip to the function bucket
gsutil cp fct_extract.zip $FUNCTION_BUCKET

# create pub/sub topic
gcloud pubsub topics create parkings_mel_topic

# job scheduler
gcloud scheduler jobs create pubsub parkings_mel_job --schedule="*/5 * * * *" --topic=parkings_mel_topic --message-body="parkings_mel message" --time-zone="Europe/Paris" --location="europe-west2"

# cloud function deployment
gcloud functions deploy parkings_mel_function --runtime=python310 --trigger-topic=parkings_mel_topic --source=$FUNCTION_BUCKET/fct_extract.zip --service-account=$SERVICE_ACCOUNT_EMAIL --region=$REGION --entry-point=parkings_mel_pubsub

