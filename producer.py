import requests
from kafka import KafkaProducer
import time
import json
from datetime import datetime

SERVER='localhost:9092'
TOPIC='weather'

producer = KafkaProducer(
    bootstrap_servers=SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serializacja JSON
)

def fetch_data_from_api():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m'
    response = requests.get(url)
    if response.status_code == 200:
        data=response.json()
        data['timestamp']=str(datetime.now())
        return data
    else:
        return None

while True:
    data = fetch_data_from_api()
    if data:
        producer.send(TOPIC, value=data)
        timestamp=str(datetime.now())
        print(f"{timestamp} Wysłano dane z API!")
    time.sleep(60)  #Wysyłanie co 1min=60sekund