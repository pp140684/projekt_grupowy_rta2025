from flask import Flask, render_template_string, request
from kafka import KafkaConsumer
import threading
import json
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import plotly.express as px
import plotly
import time

app = Flask(__name__)
messages = []
messages_lock = threading.Lock()

def consume_messages():
    consumer = KafkaConsumer(
        'stan_powietrza',
        bootstrap_servers='broker:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id=f'dash-{int(time.time())}'
    )
    print('Start Kafka')
    for message in consumer:
        data = message.value
        with messages_lock:
            messages.append(data)
            if len(messages) > 100000:  # duży bufor bezpieczeństwa
                messages.pop(0)

def render_folium_map(df, param_name):
    if df.empty or "gegrLat" not in df or "gegrLon" not in df:
        return "<b>Brak danych do mapy.</b>"
    m = folium.Map(location=[52.1, 19.4], zoom_start=6)  # środek Polski
    cluster = MarkerCluster().add_to(m)
    for _, row in df.iterrows():
        try:
            val = float(row["value"]) if row["value"] is not None else None
        except Exception:
            val = None
        # Kolorowanie
        if val is None:
            color = "gray"
        elif val < 20:
            color = "green"
        elif val < 50:
            color = "orange"
        else:
            color = "red"
        folium.CircleMarker(
            location=[float(row["gegrLat"]), float(row["gegrLon"])],
            radius=6,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=(
                f"{row.get('stationName','')}<br>"
                f"Miasto: {row.get('city','')}<br>"
                f"Parametr: {row.get('paramName','')}<br>"
                f"Data: {row.get('date','')}<br>"
                f"Wartość: {row.get('value','')}"
            )
        ).add_to(cluster)
    return m._repr_html_()

@app.route('/')
def dashboard():
    with messages_lock:
        df = pd.DataFrame(messages)
    if df.empty or "city" not in df.columns or "paramName" not in df.columns:
        return "<h2>Brak danych lub nieprawidłowa struktura wiadomości!</h2>"

    miasta = sorted(df["city"].dropna().unique())
    miasta = ["Cała Polska"] + miasta
    miasto = request.args.get("city", miasta[0])

    parametry = sorted(df["paramName"].dropna().unique())
    parametr = request.args.get("param", parametry[0])

    # FILTROWANIE wg miasta i parametru (ale BRAK deduplikacji, bierzemy wszystko)
    if miasto == "Cała Polska":
        df_city = df[df["paramName"] == parametr].copy()
    else:
        df_city = df[(df["city"] == miasto) & (df["paramName"] == parametr)].copy()
    df_city = df_city.sort_values("date")  # cała historia

    mapa_html = render_folium_map(df_city, parametr)

    wykres_html = "<b>Brak danych do wykresu.</b>"
    if not df_city.empty and "date" in df_city.columns and "value" in df_city.columns:
        try:
            df_chart = df_city.dropna(subset=["value", "date"])
            if not df_chart.empty:
                fig = px.line(
                    df_chart,
                    x="date",
                    y="value",
                    color="stationName" if miasto != "Cała Polska" else "city",
                    markers=True,
                    title=f"{parametr} – {miasto}",
                    labels={"value": f"Wartość {parametr} [µg/m³]"}
                )
                fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
                wykres_html = plotly.io.to_html(fig, include_plotlyjs="cdn", full_html=False)
        except Exception as e:
            wykres_html = f"<b>Błąd rysowania wykresu: {e}</b>"

    # TABELA – pokazuje WSZYSTKO (możesz usunąć .head(20), tabela będzie bardzo długa!)
    tabela_html = df_city[["stationName", "date", "value", "paramName"]] \
        .sort_values("date", ascending=False) \
        .to_html(index=False, classes="display", table_id="ostatnie")

    return render_template_string('''
    <!doctype html>
    <html>
    <head>
        <title>Dashboard GIOŚ</title>
        <meta http-equiv="refresh" content="900">
        <link rel="stylesheet" type="text/css"
         href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
        <script type="text/javascript"
         src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <style>
            body { font-family: Arial, sans-serif; background: #f8f9fa; }
            .flex { display: flex; gap: 2em; }
            .box { background: #fff; padding: 1em; border-radius: 8px; box-shadow: 0 2px 8px #ddd; margin-bottom: 1em; }
            .map { width: 50vw; min-width: 360px; height: 480px; }
            .side { width: 45vw; min-width: 320px;}
        </style>
    </head>
    <body>
        <h1>🌫️ Dashboard jakości powietrza GIOŚ</h1>
        <form method="get" style="margin-bottom:1em">
            Miasto:
            <select name="city">
                {% for m in miasta %}
                    <option value="{{ m }}" {% if m == miasto %}selected{% endif %}>{{ m }}</option>
                {% endfor %}
            </select>
            Parametr:
            <select name="param">
                {% for p in parametry %}
                    <option value="{{ p }}" {% if p == parametr %}selected{% endif %}>{{ p }}</option>
                {% endfor %}
            </select>
            <button type="submit">Filtruj</button>
        </form>
        <div class="flex">
            <div class="map box">{{ mapa_html|safe }}</div>
            <div class="side">
                <div class="box">{{ wykres_html|safe }}</div>
                <div class="box">
                    <h3>Wszystkie pomiary z wybranego miasta</h3>
                    {{ tabela_html|safe }}
                </div>
            </div>
        </div>
        <script>
        $(document).ready(function() {
            $('#ostatnie').DataTable({
                "order": []
            });
        });
        </script>
    </body>
    </html>
    ''',
    mapa_html=mapa_html,
    wykres_html=wykres_html,
    tabela_html=tabela_html,
    miasta=miasta,
    miasto=miasto,
    parametry=parametry,
    parametr=parametr
    )

if __name__ == '__main__':
    threading.Thread(target=consume_messages, daemon=True).start()
    print('Start wątku konsumenta')
    app.run(host="0.0.0.0", port=5000)
