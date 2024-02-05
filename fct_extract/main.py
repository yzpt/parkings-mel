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