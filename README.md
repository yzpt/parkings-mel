# ETL on GCP : Parkings de la MEL (Métropole Européenne de Lille)

# 1. GCP implementation

## 1.1. Environments, variables & misc

* journal.sh :

```bash
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
jupyter nbconvert --to markdown pandas_bigquery_select.ipynb --output-dir=notebooks_to_markdown
```

## 1.2. GCP configuration

* journal.sh :

```bash
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

```

## 1.2. Data collection

* journal.sh :

```bash
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

* nb_bigquery.ipynb:

```python
from google.cloud import bigquery
import requests
import os
import datetime
```


```python
project_id = "parkings-mel"
dataset_id = "parkings_mel"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
```


```python
client = bigquery.Client()
```


```python
# Create a 'parkings' table
table_id = project_id + '.' + dataset_id + '.parkings'

parkings_schema = [
    bigquery.SchemaField("id",          "STRING"),
    bigquery.SchemaField("name",        "STRING"),
    bigquery.SchemaField("adress",      "STRING"),
    bigquery.SchemaField("city",        "STRING"),
    bigquery.SchemaField("longitude",   "FLOAT64"),
    bigquery.SchemaField("latitude",    "FLOAT64"),
]
table = bigquery.Table(table_id, schema=parkings_schema)
table = client.create_table(table)
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

```

    Created table parkings-mel.parkings_mel.parkings



```python
# Create a 'records' table
table_id = project_id + '.' + dataset_id + '.records'
record_schema = [
    bigquery.SchemaField("station_id",              "STRING"),
    bigquery.SchemaField("state",                   "STRING"),
    bigquery.SchemaField("available",               "INT64"),
    bigquery.SchemaField("max",                     "INT64"),
    bigquery.SchemaField("display",                 "STRING"),
    bigquery.SchemaField("last_update",             "TIMESTAMP"),
    bigquery.SchemaField("record_timestamp",        "TIMESTAMP"),
]
table = bigquery.Table(table_id, schema=record_schema)
table = client.create_table(table)
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

```

    Created table parkings-mel.parkings_mel.records



```python
# request the data from the API
url = "https://metropole-europeenne-de-lille.opendatasoft.com/api/explore/v2.1/catalog/datasets/disponibilite-parkings/records?limit=100"
response = requests.get(url)
data = response.json()
```


```python
# create a dict to insert into bigquery parkings (dim) table
dict_to_insert = []
for record in data['results']:
    dict_to_insert.append({
        "id": record['id'],
        "name": record['libelle'],
        "adress": record['adresse'],
        "city": record['ville'],
        "longitude": record['geometry']['geometry']['coordinates'][0],
        "latitude": record['geometry']['geometry']['coordinates'][1],
    })
```


```python
# insert dict into bigquery parkings table
table_id = project_id + '.' + dataset_id + '.parkings'
try:
    table = client.get_table(table_id)
    errors = client.insert_rows(table, dict_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
except Exception as e:
    print(e)
    print("Error while inserting rows into the parkings table")

```

    New rows have been added.



```python
# query to check if the data has been inserted
query = """
    SELECT *
    FROM `parkings-mel.parkings_mel.parkings`
    LIMIT 5
"""
query_job = client.query(query)
results = query_job.result()
for row in results:
    print(row)
```

    Row(('LIL0012', 'Parking Rihour-Printemps', 'Place Rihour', 'Lille', 3.0613622559941, 50.635431974724), {'id': 0, 'name': 1, 'adress': 2, 'city': 3, 'longitude': 4, 'latitude': 5})
    Row(('TCG0003', 'Parking St Christophe', 'Rue des Anges', 'Tourcoing', 3.1563648534293, 50.719799610647), {'id': 0, 'name': 1, 'adress': 2, 'city': 3, 'longitude': 4, 'latitude': 5})
    Row(('LIL0006', 'Parking Euralille', '164 Avenue Willy Brandt', 'Lille', 3.0730712109881, 50.636803300033), {'id': 0, 'name': 1, 'adress': 2, 'city': 3, 'longitude': 4, 'latitude': 5})
    Row(('TCG0001', 'Parking Hotel de Ville', 'Rue de la Bienfaisance', 'Tourcoing', 3.1588907023137, 50.725413845879), {'id': 0, 'name': 1, 'adress': 2, 'city': 3, 'longitude': 4, 'latitude': 5})
    Row(('LIL0005', 'Parking Gare Lille Flandres', 'Rue de Tournai', 'Lille', 3.0724327352937, 50.634836454356), {'id': 0, 'name': 1, 'adress': 2, 'city': 3, 'longitude': 4, 'latitude': 5})



```python
# request the data from the API
url = "https://metropole-europeenne-de-lille.opendatasoft.com/api/explore/v2.1/catalog/datasets/disponibilite-parkings/records?limit=100"
response = requests.get(url)
data = response.json()

# dict to insert into bigquery records (fact) table
dict_to_insert = []
for record in data['results']:
    dict_to_insert.append({
        "station_id": record['id'],
        "state": record['etat'],
        "available": record['dispo'],
        "max": record['max'],
        "display": record['aff'],
        "last_update": record['datemaj'],
        "record_timestamp": datetime.datetime.now()
    })
```


```python
# Try to insert a record into the records table
table_id = project_id + '.' + dataset_id + '.records'
try:
    table = client.get_table(table_id)
    errors = client.insert_rows(table, dict_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
except Exception as e:
    print(e)
    print("Error while inserting rows into the records table")
```

    New rows have been added.



```python
# query to check if the data has been inserted
query = """
    SELECT *
    FROM `parkings-mel.parkings_mel.records`
    LIMIT 5
"""
query_job = client.query(query)
results = query_job.result()
for row in results:
    print(row)
```

    Row(('test', 'test', 1, 1, 'test', datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)), {'station_id': 0, 'state': 1, 'available': 2, 'max': 3, 'display': 4, 'last_update': 5, 'record_timestamp': 6})
    Row(('RBX0007', 'OUVERT', 24, 85, '20', datetime.datetime(2024, 2, 5, 21, 8, tzinfo=datetime.timezone.utc), datetime.datetime(2024, 2, 5, 22, 10, 57, 315979, tzinfo=datetime.timezone.utc)), {'station_id': 0, 'state': 1, 'available': 2, 'max': 3, 'display': 4, 'last_update': 5, 'record_timestamp': 6})
    Row(('LIL0010', 'OUVERT', 38, 246, '35', datetime.datetime(2024, 2, 5, 21, 8, tzinfo=datetime.timezone.utc), datetime.datetime(2024, 2, 5, 22, 10, 57, 315989, tzinfo=datetime.timezone.utc)), {'station_id': 0, 'state': 1, 'available': 2, 'max': 3, 'display': 4, 'last_update': 5, 'record_timestamp': 6})
    Row(('TCG0001', 'FERME', 67, 434, 'FERME', datetime.datetime(2024, 2, 5, 21, 8, tzinfo=datetime.timezone.utc), datetime.datetime(2024, 2, 5, 22, 10, 57, 315991, tzinfo=datetime.timezone.utc)), {'station_id': 0, 'state': 1, 'available': 2, 'max': 3, 'display': 4, 'last_update': 5, 'record_timestamp': 6})
    Row(('VAQ0003', '.', 0, 1000, '.', datetime.datetime(2024, 2, 5, 21, 8, tzinfo=datetime.timezone.utc), datetime.datetime(2024, 2, 5, 22, 10, 57, 315993, tzinfo=datetime.timezone.utc)), {'station_id': 0, 'state': 1, 'available': 2, 'max': 3, 'display': 4, 'last_update': 5, 'record_timestamp': 6})


``` 

## 1.3. Cloud Function

* journal.sh :

```bash
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
```

* fct_extract/main.py :

```python
from google.cloud import bigquery
import requests
import os
import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

def extract():        
    """
    Extract data from the API
    """
    
    url = "https://metropole-europeenne-de-lille.opendatasoft.com/api/explore/v2.1/catalog/datasets/disponibilite-parkings/records?limit=100"
    response = requests.get(url)
    data = response.json()
    return data

def transform(data):
    """
    Transform data into a list of dictionaries
    """
    dict_to_insert = []
    for record in data['results']:
        dict_to_insert.append({
            "station_id": record['id'],
            "state": record['etat'],
            "available": record['dispo'],
            "max": record['max'],
            "display": record['aff'],
            "last_update": record['datemaj'],
            "record_timestamp": datetime.datetime.now()
        })
    return dict_to_insert
    
def load(dict_to_insert, project_id = "parkings-mel", dataset_id = "parkings_mel"):
    """
    Load data into BigQuery
    """
    
    client = bigquery.Client()
    
    table_id = project_id + '.' + dataset_id + '.records'
    try:
        table = client.get_table(table_id)
        errors = client.insert_rows(table, dict_to_insert)
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    except Exception as e:
        print(e)
        print("Error while inserting rows into the records table")

def parkings_mel_pubsub(data, context):
    data = extract()
    dict_to_insert = transform(data)
    load(dict_to_insert)

if __name__ == "__main__":
    parkings_mel_pubsub('data', 'context')
```

* journal.sh :

```bash
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
```

# 2. SELECT queries from BigQuery using pandas/pandas-gbq

* pandas_bigquery_select.ipynb

```python
# venv_dev
"""
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
"""
```



```python
import pandas as pd
import pandas_gbq
import os
from IPython.display import display
```


```python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
```


```python

PROJECT_ID="parkings-mel"
REGION="europe-west2"
ZONE="europe-west2-a"
DATASET_NAME="parkings_mel"
```


```python
query = """
SELECT
    *
FROM
    `parkings_mel.parkings`
ORDER BY
    id
"""


df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
display(df)
```





<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>adress</th>
      <th>city</th>
      <th>longitude</th>
      <th>latitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>LIL0001</td>
      <td>Parking Republique</td>
      <td>Place de la Republique</td>
      <td>Lille</td>
      <td>3.062724</td>
      <td>50.631027</td>
    </tr>
    <tr>
      <th>1</th>
      <td>LIL0002</td>
      <td>Parking Plaza</td>
      <td>Rue Nationale</td>
      <td>Lille</td>
      <td>3.058334</td>
      <td>50.635122</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LIL0003</td>
      <td>Parking Les Tanneurs</td>
      <td>Rue du Molinel</td>
      <td>Lille</td>
      <td>3.067235</td>
      <td>50.634257</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LIL0004</td>
      <td>Parking Grand Palais</td>
      <td>Boulevard des Citees Unies</td>
      <td>Lille</td>
      <td>3.078601</td>
      <td>50.631481</td>
    </tr>
    <tr>
      <th>4</th>
      <td>LIL0005</td>
      <td>Parking Gare Lille Flandres</td>
      <td>Rue de Tournai</td>
      <td>Lille</td>
      <td>3.072433</td>
      <td>50.634836</td>
    </tr>
    <tr>
      <th>5</th>
      <td>LIL0006</td>
      <td>Parking Euralille</td>
      <td>164 Avenue Willy Brandt</td>
      <td>Lille</td>
      <td>3.073071</td>
      <td>50.636803</td>
    </tr>
    <tr>
      <th>6</th>
      <td>LIL0007</td>
      <td>Parking Tours</td>
      <td>Boulevard de Turin</td>
      <td>Lille</td>
      <td>3.076680</td>
      <td>50.638789</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LIL0008</td>
      <td>Parking Gare Lille Europe</td>
      <td>Avenue de Cologne</td>
      <td>Lille</td>
      <td>3.075970</td>
      <td>50.639770</td>
    </tr>
    <tr>
      <th>8</th>
      <td>LIL0009</td>
      <td>Parking Opera</td>
      <td>Avenue Carnot</td>
      <td>Lille</td>
      <td>3.066548</td>
      <td>50.639305</td>
    </tr>
    <tr>
      <th>9</th>
      <td>LIL0010</td>
      <td>Parking Vieux Lille</td>
      <td>Avenue du Peuple Belge</td>
      <td>Lille</td>
      <td>3.064070</td>
      <td>50.641600</td>
    </tr>
    <tr>
      <th>10</th>
      <td>LIL0011</td>
      <td>Parking Grand Place</td>
      <td>Place du General de Gaulle</td>
      <td>Lille</td>
      <td>3.063635</td>
      <td>50.637111</td>
    </tr>
    <tr>
      <th>11</th>
      <td>LIL0012</td>
      <td>Parking Rihour-Printemps</td>
      <td>Place Rihour</td>
      <td>Lille</td>
      <td>3.061362</td>
      <td>50.635432</td>
    </tr>
    <tr>
      <th>12</th>
      <td>LIL0013</td>
      <td>Parking Bethune-Lafayette</td>
      <td>6 rue de la Riviere</td>
      <td>Lille</td>
      <td>3.065059</td>
      <td>50.634141</td>
    </tr>
    <tr>
      <th>13</th>
      <td>LIL0014</td>
      <td>Parking Nouveau Siecle</td>
      <td>19 place Mendes France</td>
      <td>Lille</td>
      <td>3.059397</td>
      <td>50.637305</td>
    </tr>
    <tr>
      <th>14</th>
      <td>LIL0015</td>
      <td>Parking Liberte</td>
      <td>Facade de l'Esplanade</td>
      <td>Lille</td>
      <td>3.050744</td>
      <td>50.638697</td>
    </tr>
    <tr>
      <th>15</th>
      <td>LIL0016</td>
      <td>Parking Petit Paradis</td>
      <td>Facade Esplanade</td>
      <td>Lille</td>
      <td>3.049734</td>
      <td>50.644637</td>
    </tr>
    <tr>
      <th>16</th>
      <td>RBX0001</td>
      <td>Parking CHURCHILL</td>
      <td>Rue du president Vincent Auriol</td>
      <td>Roubaix</td>
      <td>3.178875</td>
      <td>50.689338</td>
    </tr>
    <tr>
      <th>17</th>
      <td>RBX0002</td>
      <td>Parking Mac Arthur Glen (Lannoy)</td>
      <td>Rue Charles Watteeuw</td>
      <td>Roubaix</td>
      <td>3.181234</td>
      <td>50.690973</td>
    </tr>
    <tr>
      <th>18</th>
      <td>RBX0003</td>
      <td>Parking Mac Arthur Glen (Gambetta)</td>
      <td>Rue Charles Watteeuw</td>
      <td>Roubaix</td>
      <td>3.179490</td>
      <td>50.690951</td>
    </tr>
    <tr>
      <th>19</th>
      <td>RBX0005</td>
      <td>Parking LA POSTE</td>
      <td>Rue de la Halle</td>
      <td>Roubaix</td>
      <td>3.176009</td>
      <td>50.690320</td>
    </tr>
    <tr>
      <th>20</th>
      <td>RBX0006</td>
      <td>Parking GRAND RUE</td>
      <td>Avenue des Nations Unies</td>
      <td>Roubaix</td>
      <td>3.175681</td>
      <td>50.693330</td>
    </tr>
    <tr>
      <th>21</th>
      <td>RBX0007</td>
      <td>Parking CENTRE</td>
      <td>Rue Neuve</td>
      <td>Roubaix</td>
      <td>3.172127</td>
      <td>50.691087</td>
    </tr>
    <tr>
      <th>22</th>
      <td>TCG0001</td>
      <td>Parking Hotel de Ville</td>
      <td>Rue de la Bienfaisance</td>
      <td>Tourcoing</td>
      <td>3.158891</td>
      <td>50.725414</td>
    </tr>
    <tr>
      <th>23</th>
      <td>TCG0002</td>
      <td>Parking Miss Cavell</td>
      <td>Avenue Allende</td>
      <td>Tourcoing</td>
      <td>3.164869</td>
      <td>50.722540</td>
    </tr>
    <tr>
      <th>24</th>
      <td>TCG0003</td>
      <td>Parking St Christophe</td>
      <td>Rue des Anges</td>
      <td>Tourcoing</td>
      <td>3.156365</td>
      <td>50.719800</td>
    </tr>
    <tr>
      <th>25</th>
      <td>VAQ0001</td>
      <td>Parking TRIOLO</td>
      <td>Avenue Paul Langevin</td>
      <td>Villeneuve d'Ascq</td>
      <td>3.138413</td>
      <td>50.614282</td>
    </tr>
    <tr>
      <th>26</th>
      <td>VAQ0002</td>
      <td>Parking 4CANTONS</td>
      <td>Avenue Pointcare</td>
      <td>Villeneuve d'Ascq</td>
      <td>3.138176</td>
      <td>50.604623</td>
    </tr>
    <tr>
      <th>27</th>
      <td>VAQ0003</td>
      <td>Parking LESPRES</td>
      <td>Boulevard de l'Ouest</td>
      <td>Villeneuve d'Ascq</td>
      <td>3.124203</td>
      <td>50.649216</td>
    </tr>
  </tbody>
</table>
</div>



```python
query = """
SELECT
    *
FROM
    `parkings_mel.records`
    LIMIT 10
"""

df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
display(df)
```





<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>station_id</th>
      <th>state</th>
      <th>available</th>
      <th>max</th>
      <th>display</th>
      <th>last_update</th>
      <th>record_timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>LIL0003</td>
      <td>OUVERT</td>
      <td>321</td>
      <td>563</td>
      <td>320</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040068+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>LIL0010</td>
      <td>OUVERT</td>
      <td>38</td>
      <td>246</td>
      <td>35</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040080+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LIL0013</td>
      <td>OUVERT</td>
      <td>217</td>
      <td>465</td>
      <td>215</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040083+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LIL0008</td>
      <td>OUVERT</td>
      <td>517</td>
      <td>600</td>
      <td>515</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040086+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>RBX0006</td>
      <td>COMPLET</td>
      <td>0</td>
      <td>0</td>
      <td>COMPLET</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040089+00:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>LIL0016</td>
      <td>OUVERT</td>
      <td>250</td>
      <td>270</td>
      <td>250</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040091+00:00</td>
    </tr>
    <tr>
      <th>6</th>
      <td>RBX0001</td>
      <td>COMPLET</td>
      <td>0</td>
      <td>0</td>
      <td>COMPLET</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040094+00:00</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LIL0007</td>
      <td>OUVERT</td>
      <td>68</td>
      <td>160</td>
      <td>65</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040097+00:00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>TCG0003</td>
      <td>LIBRE</td>
      <td>704</td>
      <td>757</td>
      <td>LIBRE</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040100+00:00</td>
    </tr>
    <tr>
      <th>9</th>
      <td>LIL0002</td>
      <td>OUVERT</td>
      <td>166</td>
      <td>323</td>
      <td>165</td>
      <td>2024-02-11 14:56:00+00:00</td>
      <td>2024-02-11 15:00:02.040103+00:00</td>
    </tr>
  </tbody>
</table>
</div>



```python
# query that select all the last records of each parking
query = """
SELECT
    *
FROM
    `parkings_mel.records` records
    WHERE record_timestamp = (SELECT MAX(record_timestamp) FROM `parkings_mel.records` WHERE station_id = records.station_id)
    order by station_id

"""

df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
display(df)
```





<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>station_id</th>
      <th>state</th>
      <th>available</th>
      <th>max</th>
      <th>display</th>
      <th>last_update</th>
      <th>record_timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>LIL0001</td>
      <td>OUVERT</td>
      <td>226</td>
      <td>300</td>
      <td>205</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300907+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>LIL0002</td>
      <td>OUVERT</td>
      <td>170</td>
      <td>323</td>
      <td>170</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300894+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LIL0003</td>
      <td>OUVERT</td>
      <td>325</td>
      <td>563</td>
      <td>325</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300918+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LIL0004</td>
      <td>OUVERT</td>
      <td>381</td>
      <td>1182</td>
      <td>380</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300883+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>LIL0005</td>
      <td>OUVERT</td>
      <td>149</td>
      <td>374</td>
      <td>145</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300927+00:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>LIL0006</td>
      <td>OUVERT</td>
      <td>1979</td>
      <td>2873</td>
      <td>1975</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300891+00:00</td>
    </tr>
    <tr>
      <th>6</th>
      <td>LIL0007</td>
      <td>OUVERT</td>
      <td>72</td>
      <td>160</td>
      <td>70</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300939+00:00</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LIL0008</td>
      <td>OUVERT</td>
      <td>513</td>
      <td>600</td>
      <td>510</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300868+00:00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>LIL0009</td>
      <td>OUVERT</td>
      <td>177</td>
      <td>450</td>
      <td>175</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300913+00:00</td>
    </tr>
    <tr>
      <th>9</th>
      <td>LIL0010</td>
      <td>OUVERT</td>
      <td>32</td>
      <td>246</td>
      <td>30</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300921+00:00</td>
    </tr>
    <tr>
      <th>10</th>
      <td>LIL0011</td>
      <td>OUVERT</td>
      <td>16</td>
      <td>342</td>
      <td>15</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300915+00:00</td>
    </tr>
    <tr>
      <th>11</th>
      <td>LIL0012</td>
      <td>OUVERT</td>
      <td>295</td>
      <td>300</td>
      <td>295</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300856+00:00</td>
    </tr>
    <tr>
      <th>12</th>
      <td>LIL0013</td>
      <td>OUVERT</td>
      <td>218</td>
      <td>465</td>
      <td>215</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300924+00:00</td>
    </tr>
    <tr>
      <th>13</th>
      <td>LIL0014</td>
      <td>OUVERT</td>
      <td>191</td>
      <td>733</td>
      <td>190</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300902+00:00</td>
    </tr>
    <tr>
      <th>14</th>
      <td>LIL0015</td>
      <td>OUVERT</td>
      <td>514</td>
      <td>570</td>
      <td>510</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300871+00:00</td>
    </tr>
    <tr>
      <th>15</th>
      <td>LIL0016</td>
      <td>OUVERT</td>
      <td>250</td>
      <td>270</td>
      <td>250</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300936+00:00</td>
    </tr>
    <tr>
      <th>16</th>
      <td>RBX0001</td>
      <td>COMPLET</td>
      <td>0</td>
      <td>0</td>
      <td>COMPLET</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300910+00:00</td>
    </tr>
    <tr>
      <th>17</th>
      <td>RBX0002</td>
      <td>OUVERT</td>
      <td>719</td>
      <td>403</td>
      <td>520</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300930+00:00</td>
    </tr>
    <tr>
      <th>18</th>
      <td>RBX0003</td>
      <td>COMPLET</td>
      <td>0</td>
      <td>0</td>
      <td>COMPLET</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300865+00:00</td>
    </tr>
    <tr>
      <th>19</th>
      <td>RBX0005</td>
      <td>OUVERT</td>
      <td>65</td>
      <td>82</td>
      <td>65</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300877+00:00</td>
    </tr>
    <tr>
      <th>20</th>
      <td>RBX0006</td>
      <td>COMPLET</td>
      <td>0</td>
      <td>0</td>
      <td>COMPLET</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300874+00:00</td>
    </tr>
    <tr>
      <th>21</th>
      <td>RBX0007</td>
      <td>OUVERT</td>
      <td>29</td>
      <td>85</td>
      <td>25</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300885+00:00</td>
    </tr>
    <tr>
      <th>22</th>
      <td>TCG0001</td>
      <td>OUVERT</td>
      <td>35</td>
      <td>434</td>
      <td>35</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300899+00:00</td>
    </tr>
    <tr>
      <th>23</th>
      <td>TCG0002</td>
      <td>FERME</td>
      <td>59</td>
      <td>273</td>
      <td>FERME</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300880+00:00</td>
    </tr>
    <tr>
      <th>24</th>
      <td>TCG0003</td>
      <td>OUVERT</td>
      <td>691</td>
      <td>757</td>
      <td>691</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300888+00:00</td>
    </tr>
    <tr>
      <th>25</th>
      <td>VAQ0001</td>
      <td>FERME</td>
      <td>0</td>
      <td>1400</td>
      <td>FERME</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300897+00:00</td>
    </tr>
    <tr>
      <th>26</th>
      <td>VAQ0002</td>
      <td>OUVERT</td>
      <td>978</td>
      <td>2000</td>
      <td>975</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300933+00:00</td>
    </tr>
    <tr>
      <th>27</th>
      <td>VAQ0003</td>
      <td>.</td>
      <td>0</td>
      <td>1000</td>
      <td>.</td>
      <td>2024-02-11 15:42:00+00:00</td>
      <td>2024-02-11 15:45:02.300904+00:00</td>
    </tr>
  </tbody>
</table>
</div>



```python
# query to get the number of available places for the station_id = LIL0003, ordered by distinct last_update
query = """
SELECT
    station_id, 
    parkings.name, 
    parkings.city, 
    records.state,
    records.available,
    records.max,
    records.last_update
FROM
    `parkings_mel.records` records
JOIN
    `parkings_mel.parkings` parkings
  ON records.station_id = parkings.id
WHERE 
  ( station_id = 'LIL0003'  OR station_id = 'LIL0009')
  AND last_update >= '2024-02-07'
ORDER BY last_update DESC
"""

df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
display(df.head())
```





<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>station_id</th>
      <th>name</th>
      <th>city</th>
      <th>state</th>
      <th>available</th>
      <th>max</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>LIL0003</td>
      <td>Parking Les Tanneurs</td>
      <td>Lille</td>
      <td>OUVERT</td>
      <td>326</td>
      <td>563</td>
      <td>2024-02-11 15:46:00+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>LIL0009</td>
      <td>Parking Opera</td>
      <td>Lille</td>
      <td>OUVERT</td>
      <td>176</td>
      <td>450</td>
      <td>2024-02-11 15:46:00+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LIL0009</td>
      <td>Parking Opera</td>
      <td>Lille</td>
      <td>OUVERT</td>
      <td>177</td>
      <td>450</td>
      <td>2024-02-11 15:42:00+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LIL0003</td>
      <td>Parking Les Tanneurs</td>
      <td>Lille</td>
      <td>OUVERT</td>
      <td>325</td>
      <td>563</td>
      <td>2024-02-11 15:42:00+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>LIL0003</td>
      <td>Parking Les Tanneurs</td>
      <td>Lille</td>
      <td>OUVERT</td>
      <td>324</td>
      <td>563</td>
      <td>2024-02-11 15:36:00+00:00</td>
    </tr>
  </tbody>
</table>
</div>



```python
import matplotlib.pyplot as plt
import pandas as pd

# Convert last_update to datetime
df['last_update'] = pd.to_datetime(df['last_update'])

# Sort dataframe by last_update
df.sort_values('last_update', inplace=True)

# Filter data for each station
df_LIL0003 = df[df['station_id'] == 'LIL0003']
df_LIL0009 = df[df['station_id'] == 'LIL0009']

# Plotting
plt.figure(figsize=(7,4))

# Plot for LIL0003
plt.plot(df_LIL0003['last_update'], df_LIL0003['available'], label='LIL0003')

# Plot for LIL0009
plt.plot(df_LIL0009['last_update'], df_LIL0009['available'], label='LIL0009')

# Generate midnight times
midnights = pd.date_range(start=df['last_update'].min(), end=df['last_update'].max(), freq='D', normalize=True)

# Add vertical lines at each midnight
for midnight in midnights:
    plt.axvline(x=midnight, color='r', linestyle='--')

plt.xlabel('Last Update')
plt.ylabel('Available places')
plt.legend()
plt.show()
```


    
![png](/notebooks_to_markdown/pandas_bigquery_select_files/pandas_bigquery_select_8_0.png)
