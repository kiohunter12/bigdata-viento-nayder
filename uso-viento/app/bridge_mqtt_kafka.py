"""Puente MQTT -> Kafka (mismo patrón que uso-atmos del curso).

No valida ni transforma: reenvía el payload crudo a `viento-eventos`
con key = sensorId. La validación vive en un solo lugar (consumer_viento.py).
"""
import json
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

from comun import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_VIENTO, MQTT_HOST, MQTT_PORT, MQTT_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: value,
)


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(json.dumps({"service": "uso-viento", "component": "bridge", "mqttHost": MQTT_HOST,
                      "mqttTopic": MQTT_TOPIC,
                      "status": "connected" if reason_code == 0 else f"connect_failed:{reason_code}"}))
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    raw = msg.payload
    try:
        sensor_id = json.loads(raw.decode("utf-8")).get("sensorId") or "desconocido"
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        sensor_id = "desconocido"
    md = producer.send(KAFKA_TOPIC_VIENTO, key=sensor_id, value=raw).get(timeout=10)
    print(json.dumps({"service": "uso-viento", "component": "bridge", "mqttTopic": msg.topic,
                      "kafkaTopic": md.topic, "partition": md.partition, "offset": md.offset,
                      "sensorId": sensor_id, "status": "forwarded"}))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"viento-bridge-{uuid.uuid4().hex[:8]}")
client.on_connect = on_connect
client.on_message = on_message
print(json.dumps({"service": "uso-viento", "component": "bridge", "mqttHost": MQTT_HOST,
                  "kafka": KAFKA_BOOTSTRAP_SERVERS, "status": "starting"}))
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
