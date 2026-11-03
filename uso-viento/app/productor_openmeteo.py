"""Productor IoT REAL: lee el viento actual de Open-Meteo y lo publica.

Fuente: https://api.open-meteo.com/v1/forecast  (parámetro `current=`)
  - Es la MISMA API de la Unidad I (allí se usó el endpoint histórico).
  - `current` se recalcula cada 15 min (dato de modelo, no estación física).
  - Consultamos cada INTERVALO_S (60 s por defecto): si el modelo aún no
    cambió, `obsTime` se repite -> Spark lo deduplica por (sensorId, obsTime).

Modos:
  python productor_openmeteo.py            -> publica por MQTT (patrón S7: dispositivo -> MQTT -> bridge -> Kafka)
  python productor_openmeteo.py --kafka    -> publica directo a Kafka (plan B si el broker MQTT público falla)
  python productor_openmeteo.py --una-vez  -> una sola consulta (para probar)
"""
import json
import sys
import time

import requests

from comun import (
    INTERVALO_S, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_VIENTO,
    MQTT_HOST, MQTT_PORT, MQTT_TOPIC, SENSORES,
)

URL = "https://api.open-meteo.com/v1/forecast"
VARIABLES = (
    "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
    "temperature_2m,relative_humidity_2m,surface_pressure"
)


def leer_open_meteo():
    """Una sola llamada HTTP para los 3 puntos (Open-Meteo acepta listas)."""
    params = {
        "latitude": ",".join(str(s["lat"]) for s in SENSORES),
        "longitude": ",".join(str(s["lon"]) for s in SENSORES),
        "current": VARIABLES,
        "wind_speed_unit": "kmh",
        "timeformat": "unixtime",
        "timezone": "GMT",
    }
    resp = requests.get(URL, params=params, timeout=20)
    resp.raise_for_status()
    datos = resp.json()
    if isinstance(datos, dict):  # con un solo punto devuelve dict, con varios lista
        datos = [datos]

    ahora_ms = int(time.time() * 1000)
    eventos = []
    for sensor, punto in zip(SENSORES, datos):
        c = punto["current"]
        eventos.append({
            "tipoEvento": "viento.lectura",
            "version": 1,
            "sensorId": sensor["sensorId"],
            "estacion": sensor["estacion"],
            "lat": sensor["lat"],
            "lon": sensor["lon"],
            "velocidadViento": c["wind_speed_10m"],
            "direccionViento": c["wind_direction_10m"],
            "rafaga": c["wind_gusts_10m"],
            "temperatura": c["temperature_2m"],
            "humedad": c["relative_humidity_2m"],
            "presion": c["surface_pressure"],
            "obsTime": int(c["time"]) * 1000,
            "origen": "open-meteo",
            "timestamp": ahora_ms,
        })
    return eventos


def crear_publicador(modo_kafka):
    if modo_kafka:
        from kafka import KafkaProducer
        prod = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        def publicar(evento):
            md = prod.send(KAFKA_TOPIC_VIENTO, key=evento["sensorId"], value=evento).get(timeout=10)
            return {"destino": "kafka", "topic": md.topic, "partition": md.partition, "offset": md.offset}
        return publicar

    import paho.mqtt.client as mqtt
    cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"viento-productor-{int(time.time())}")
    cli.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    cli.loop_start()

    def publicar(evento):
        info = cli.publish(MQTT_TOPIC, json.dumps(evento), qos=1)
        info.wait_for_publish(timeout=10)
        return {"destino": "mqtt", "mqttTopic": MQTT_TOPIC, "mid": info.mid}
    return publicar


def main():
    modo_kafka = "--kafka" in sys.argv
    una_vez = "--una-vez" in sys.argv
    publicar = crear_publicador(modo_kafka)
    print(json.dumps({"service": "uso-viento", "component": "productor", "fuente": URL,
                      "modo": "kafka" if modo_kafka else "mqtt", "intervaloS": INTERVALO_S,
                      "status": "starting"}))
    while True:
        try:
            for ev in leer_open_meteo():
                meta = publicar(ev)
                print(json.dumps({
                    "service": "uso-viento", "component": "productor", **meta,
                    "sensorId": ev["sensorId"], "velocidadViento": ev["velocidadViento"],
                    "rafaga": ev["rafaga"], "direccionViento": ev["direccionViento"],
                    "obsTime": ev["obsTime"], "status": "published",
                }))
        except Exception as ex:  # la API o la red pueden fallar: se registra y se reintenta
            print(json.dumps({"service": "uso-viento", "component": "productor",
                              "status": "error", "error": str(ex)}))
        if una_vez:
            break
        time.sleep(INTERVALO_S)


if __name__ == "__main__":
    main()
