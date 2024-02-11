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



```python

```
