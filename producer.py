import requests
from kafka import KafkaProducer
import json
from datetime import datetime
import time

SERVER = 'broker:9092'
TOPIC = 'stan_powietrza'

producer = KafkaProducer(
    bootstrap_servers=SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_all():
    stations = requests.get('https://api.gios.gov.pl/pjp-api/rest/station/findAll').json()
    for st in stations:
        station_id = st["id"]
        sensors_url = f"https://api.gios.gov.pl/pjp-api/rest/station/sensors/{station_id}"
        sensors = requests.get(sensors_url).json()
        for sensor in sensors:
            sensor_id = sensor["id"]
            param = sensor["param"]["paramName"]
            data_url = f"https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}"
            data = requests.get(data_url).json()
            for v in data["values"]:
                if v["value"] is not None:
                    record = {
                        "stationId": station_id,
                        "stationName": st.get("stationName"),
                        "city": st.get("city", {}).get("commune", {}).get("communeName"),
                        "voivodeship": st.get("city", {}).get("commune", {}).get("provinceName"),
                        "gegrLat": st.get("gegrLat"),
                        "gegrLon": st.get("gegrLon"),
                        "sensorId": sensor_id,
                        "paramName": param,
                        "date": v["date"],
                        "value": v["value"]
                    }
                    producer.send(TOPIC, value=record)
            time.sleep(0.5)
        print(f"Stacja {station_id} OK ({st.get('stationName')})")
        time.sleep(1)

while True:
    fetch_all()
    print(f"[{datetime.now()}] Wysłano wszystkie dostępne dane do Kafki!")
    time.sleep(60 * 60)  # co 1h
