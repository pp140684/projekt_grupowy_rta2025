from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer
import threading
import json
import time

app = Flask(__name__)
messages = []
messages_lock = threading.Lock()

def consume_messages():
    consumer = KafkaConsumer(
        'weather',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print('Start Kafka')
    
    for message in consumer:
        data = message.value
        print('Otrzymano wiadomość',data['timestamp'])
        max_temp=max(data['hourly']['temperature_2m'])
        with messages_lock:
            messages.append(data)
        print(data['timestamp'],max_temp)


@app.route('/')
def home():
    with messages_lock:
        if messages:
            temperatura = str(max(messages[-1]['hourly']['temperature_2m']))
            timestamp=messages[-1]['timestamp']
        else:
            temperatura = 'Brak danych'
            timestamp = 'Brak danych'

    return render_template_string('''
        <!doctype html>
        <html>
        <head>
            <meta http-equiv="refresh" content="5">  <!-- Auto odświeżanie co 5 sekund -->
            <title>Flask + Kafka</title>
            <style>
                body { font-family: sans-serif; margin: 2em; }
                h1 { color: #3a86ff; }
            </style>
        </head>
        <body>
            <h1>🌤️ Flask + Kafka</h1>
            <p>Ostatnia wiadomość otrzymana: <strong>{{ timestamp }}</strong></p>                          
            <p>Ostatnia temperatura: <strong>{{ temp }}</strong> °C</p>
        </body>
        </html>
    ''', temp=temperatura, timestamp=timestamp)

#Start wątków i Flaska
if __name__ == '__main__':
    threading.Thread(target=consume_messages, daemon=True).start()
    print('Start Wątków')

app.run(debug=True, port=5000)