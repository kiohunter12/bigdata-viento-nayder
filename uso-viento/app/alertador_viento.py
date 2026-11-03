"""Servicio alertador: une el flujo IoT con el flujo de negocio.

Lee `viento-eventos` (lecturas reales) y, cuando la ráfaga supera el
umbral operativo, publica el evento de negocio `alerta.emitida` en
`alerta-eventos` (key = estacion).

Como el productor consulta Open-Meteo cada 60 s pero el modelo cambia cada
15 min, la misma lectura llega varias veces: se emite UNA alerta por
(sensorId, obsTime) -> idempotencia del lado del negocio.
"""
import json
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer

from comun import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTAS, KAFKA_TOPIC_VIENTO, UMBRAL_RAFAGA_KMH,
    fuera_de_rango, nivel_alerta, validar_esquema,
)

consumer = KafkaConsumer(
    KAFKA_TOPIC_VIENTO,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="alertador-viento-group",
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

ya_alertado = set()
print(json.dumps({"service": "uso-viento", "component": "alertador", "umbralRafagaKmh": UMBRAL_RAFAGA_KMH,
                  "status": "listening"}))

for msg in consumer:
    try:
        ev = json.loads(msg.value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        continue  # el consumer_viento ya registra los inválidos; aquí solo se ignoran
    if validar_esquema(ev) or fuera_de_rango(ev):
        continue
    nivel = nivel_alerta(ev["rafaga"])
    clave = (ev["sensorId"], ev["obsTime"])
    if not nivel or clave in ya_alertado:
        continue
    ya_alertado.add(clave)

    alerta = {
        "tipoEvento": "alerta.emitida",
        "version": 1,
        "alertaId": f"ALR-{uuid.uuid4().hex[:8]}",
        "estacion": ev["estacion"],
        "sensorId": ev["sensorId"],
        "nivel": nivel,
        "rafaga": ev["rafaga"],
        "umbral": UMBRAL_RAFAGA_KMH,
        "obsTime": ev["obsTime"],
        "origen": "alertador-viento",
        "timestamp": int(time.time() * 1000),
    }
    md = producer.send(KAFKA_TOPIC_ALERTAS, key=ev["estacion"], value=alerta).get(timeout=10)
    print(json.dumps({"service": "uso-viento", "component": "alertador", "topic": md.topic,
                      "partition": md.partition, "offset": md.offset, "alertaId": alerta["alertaId"],
                      "estacion": alerta["estacion"], "nivel": nivel, "rafaga": ev["rafaga"],
                      "status": "published"}))
