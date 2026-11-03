"""Productor de EVENTOS DE NEGOCIO (S6): `suscripcion.creada`.

Caso: un productor agrícola (p. ej. de chuño o pastos) registra su parcela
en una estación para recibir avisos de viento fuerte. Es un evento de
negocio (lo dispara una persona, baja frecuencia), a diferencia de
`viento.lectura` (lo dispara un dispositivo, alta frecuencia).

Uso:
  python producer_suscripciones.py                       -> 3 suscripciones de demo (una por estación)
  python producer_suscripciones.py P-010 Juliaca 945123456
"""
import json
import sys
import time
import uuid

from kafka import KafkaProducer

from comun import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTAS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

if len(sys.argv) == 4:
    solicitudes = [(sys.argv[1], sys.argv[2], sys.argv[3])]
else:  # datos de DEMO (no son personas reales)
    solicitudes = [("P-001", "Juliaca", "900000001"), ("P-002", "Puno", "900000002"),
                   ("P-003", "Lampa", "900000003")]

for productor_id, estacion, celular in solicitudes:
    evento = {
        "tipoEvento": "suscripcion.creada",
        "version": 1,
        "suscripcionId": f"SUS-{uuid.uuid4().hex[:8]}",
        "productorId": productor_id,
        "estacion": estacion,
        "canal": "SMS",
        "contacto": celular,
        "origen": "producer-suscripciones",
        "timestamp": int(time.time() * 1000),
    }
    md = producer.send(KAFKA_TOPIC_ALERTAS, key=estacion, value=evento).get(timeout=10)
    print(json.dumps({"service": "uso-viento", "component": "producer-suscripciones",
                      "topic": md.topic, "partition": md.partition, "offset": md.offset,
                      "key": estacion, "eventType": evento["tipoEvento"],
                      "suscripcionId": evento["suscripcionId"], "status": "published"}))

producer.flush()
