"""Consumidor IoT con validación separada (S7).

Estados de cada mensaje:
  invalid      -> JSON malformado o faltan campos del contrato (validación de ESQUEMA)
  fuera_rango  -> esquema OK pero un valor es físicamente imposible (validación de RANGO)
  alerta       -> dato válido y real, pero la ráfaga supera el umbral operativo
  consumed     -> todo OK
"""
import json
import os
import time

from kafka import KafkaConsumer

from comun import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_VIENTO, UMBRAL_RAFAGA_KMH,
    fuera_de_rango, nivel_alerta, validar_esquema,
)

GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uso-viento-group")


def deserializar(value):
    texto = value.decode("utf-8", errors="replace")
    try:
        return json.loads(texto), texto, None
    except json.JSONDecodeError as ex:
        return None, texto, str(ex)


consumer = KafkaConsumer(
    KAFKA_TOPIC_VIENTO,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=deserializar,
    key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
)

print(json.dumps({"service": "uso-viento", "component": "consumer", "topic": KAFKA_TOPIC_VIENTO,
                  "groupId": GROUP_ID, "umbralRafagaKmh": UMBRAL_RAFAGA_KMH, "status": "listening"}))

for msg in consumer:
    evento, crudo, error_json = msg.value
    procesado = int(time.time() * 1000)
    faltantes = validar_esquema(evento) if error_json is None else ["<json malformado>"]
    malas = fuera_de_rango(evento) if not faltantes else []
    nivel = nivel_alerta(evento["rafaga"]) if not faltantes and not malas else None

    if faltantes:
        estado = "invalid"
    elif malas:
        estado = "fuera_rango"
    elif nivel:
        estado = "alerta"
    else:
        estado = "consumed"

    ev = evento if isinstance(evento, dict) else {}
    ts = ev.get("timestamp")
    print(json.dumps({
        "service": "uso-viento", "component": "consumer",
        "topic": msg.topic, "partition": msg.partition, "offset": msg.offset, "key": msg.key,
        "sensorId": ev.get("sensorId"), "velocidadViento": ev.get("velocidadViento"),
        "rafaga": ev.get("rafaga"), "direccionViento": ev.get("direccionViento"),
        "obsTime": ev.get("obsTime"), "timestamp": ts,
        "latencyMs": procesado - ts if isinstance(ts, (int, float)) else None,
        "camposFaltantes": faltantes or None, "fueraDeRango": malas or None,
        "nivelAlerta": nivel, "decodeError": error_json,
        "rawPayload": crudo if estado in ("invalid", "fuera_rango") else None,
        "status": estado,
    }))
