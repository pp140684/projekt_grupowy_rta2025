from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)
messages = []
messages_lock = threading.Lock()

def consume_messages():
    consumer = KafkaConsumer(
        'stan_powietrza',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print('Start Kafka')

    for message in consumer:
        try:
            data = message.value  # już sparsowany JSON jako dict/lista
            print('Otrzymano wiadomość:')
            print(json.dumps(data, indent=2, ensure_ascii=False))  # czytelny print
            with messages_lock:
                messages.append(data)
        except Exception as e:
            print(f'Błąd podczas przetwarzania wiadomości: {e}')


@app.route('/')
def home():
    with messages_lock:
        if messages:
            last_message = messages[-1]
            pretty_json = json.dumps(last_message, indent=2, ensure_ascii=False)
        else:
            pretty_json = 'Brak danych'

    return render_template_string('''
        <!doctype html>
        <html>
        <head>
            <meta http-equiv="refresh" content="5">  <!-- Auto-refresh co 5 sek -->
            <title>Flask + Kafka</title>
            <style>
                body { font-family: monospace; white-space: pre-wrap; margin: 2em; background: #f9f9f9; color: #333; }
                h1 { color: #3a86ff; font-family: sans-serif; }
                .box { background: #fff; border: 1px solid #ccc; padding: 1em; border-radius: 5px; }
            </style>
        </head>
        <body>
            <h1>🧾 Ostatnia wiadomość z Kafka</h1>
            <div class="box">{{ json_data }}</div>
        </body>
        </html>
    ''', json_data=pretty_json)

# Start konsumenta i serwera Flask
if __name__ == '__main__':
    threading.Thread(target=consume_messages, daemon=True).start()
    print('Start Wątków')

    app.run(debug=True, port=5000)