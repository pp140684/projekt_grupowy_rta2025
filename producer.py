import requests
from kafka import KafkaProducer
import time
import json
from datetime import datetime
import pandas as pd

SERVER='localhost:9092'
TOPIC='stan_powietrza'

producer = KafkaProducer(
    bootstrap_servers=SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serializacja JSON
)

def fetch_data_from_api():
    url = 'https://api.gios.gov.pl/pjp-api/rest/station/findAll'
    print('Pobieram dostępne stacje')
    response = requests.get(url)
    if response.status_code == 200:
        data=response.json()

        #Tutaj obrabianie przy pomocy pandas
        data=pd.DataFrame(data)
        normal=pd.json_normalize(data['city'])
        normal=normal.rename(columns={'id':'city'})
        normal=normal.drop_duplicates()
        data['city']=data['city'].apply(lambda x:x['id'])
        data=pd.merge(data,normal,on='city',how='left')

        print('Pobieram dostępne pomiary')
        #Tutaj pobranie dokładnych pomiarów i ich obrobienie
        pomiary=data['id'].apply(lambda x:requests.get(f'https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/{x}'))
        pomiary=pomiary.apply(lambda x:x.json() if x.status_code==200 else None)
        pomiary=pd.json_normalize(pomiary)
        pomiary=pomiary.dropna(how='all')
        pomiary['id']=pomiary['id'].astype(int)

        #Finalne połączenie
        data=pd.merge(data,pomiary,how='left',on='id')

        return data.to_dict(orient='records')
    else:
        return None

while True:
    data = fetch_data_from_api()
    if data:
        producer.send(TOPIC, value=data)
        timestamp=str(datetime.now())
        print(f"{timestamp} Wysłano dane z API!")
    time.sleep(900)  #Wysyłanie co 15min=900sekund