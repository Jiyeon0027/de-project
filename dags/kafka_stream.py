from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


default_args = {
    "owner": "jiyeon",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1
}

def format_data(data):
    return {
        "name_title": data['name']['title'],
        "name_first": data['name']['first'],
        "name_last": data['name']['last'],
        "email": data['email'],
        "gender": data['gender'],
        "age": data['dob']['age'],
        "birth_date": data['dob']['date'],
        "country": data['location']['country'],
        "state": data['location']['state'],
        "city": data['location']['city'],
        "street_name": data['location']['street']['name'],
        "street_number": data['location']['street']['number'],
        "postcode": data['location']['postcode'],
        "latitude": data['location']['coordinates']['latitude'],
        "longitude": data['location']['coordinates']['longitude'],
        "timezone_offset": data['location']['timezone']['offset'],
        "timezone_desc": data['location']['timezone']['description'],
        "phone": data['phone'],
        "cell": data['cell'],
        "id_name": data['id']['name'],
        "id_value": data['id']['value'],
        "picture_large": data['picture']['large'],
        "picture_medium": data['picture']['medium'],
        "picture_thumbnail": data['picture']['thumbnail'],
        "nat": data['nat'],
        "username": data['login']['username'],
        "registered_date": data['registered']['date'],
        "registered_age": data['registered']['age'],
    }

def stream_data():
    import json
    import requests
    import time
    from confluent_kafka import Producer
    
    producer = Producer({
        "bootstrap.servers": "broker:29092",
    })
    
    url = "https://randomuser.me/api/"
    response = requests.get(url)
    res = response.json()['results'][0]
    res = format_data(res)
    
    producer.produce(topic="users_created", value=json.dumps(res).encode("utf-8"), max_block_ms=1000)
    producer.flush()
    
stream_data()

with DAG(
    dag_id="kafka_stream",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    streaming_task = PythonOperator(
        task_id="streaming_from_api",
        python_callable=stream_data
    )


