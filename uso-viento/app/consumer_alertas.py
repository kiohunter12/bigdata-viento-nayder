"""Servicio de notificaciones (consumidor + productor de negocio, S6).

Lee `alerta-eventos`:
  suscripcion.creada -> guarda el suscriptor por estación
  alerta.emitida     -> regla determinista: cuenta suscriptores de esa estación
                        y publica el TERCER evento propio `alerta.notificada`
  alerta.notificada  -> solo se registra
  cualquier otra cosa o JSON roto -> status "invalid", el consumidor NO se cae
"""
import json
import time

from kafka import KafkaConsumer, KafkaProducer

from comun import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTAS

GROUP_ID = "notificaciones-group"
consumer = KafkaConsumer(
    KAFKA_TOPIC_ALERTAS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

suscriptores = {}  # estacion -> set(productorId)
print(json.dumps({"service": "uso-viento", "component": "notificaciones", "topic": KAFKA_TOPIC_ALERTAS,
                  "groupId": GROUP_ID, "status": "listening"}))

for msg in consumer:
    base = {"service": "uso-viento", "component": "notificaciones", "topic": msg.topic,
            "partition": msg.partition, "offset": msg.offset,
            "key": msg.key.decode("utf-8") if msg.key else None}
    try:
        ev = json.loads(msg.value.decode("utf-8"))
        tipo = ev["tipoEvento"]
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError) as ex:
        print(json.dumps({**base, "status": "invalid", "error": f"{type(ex).__name__}: {ex}",
                          "rawPayload": msg.value.decode("utf-8", errors="replace")}))
        continue

    if tipo == "suscripcion.creada":
        suscriptores.setdefault(ev["estacion"], set()).add(ev["productorId"])
        print(json.dumps({**base, "eventType": tipo, "estacion": ev["estacion"],
                          "productorId": ev["productorId"], "status": "consumed"}))
    elif tipo == "alerta.emitida":
        destinatarios = sorted(suscriptores.get(ev["estacion"], set()))
        notif = {
            "tipoEvento": "alerta.notificada",
            "version": 1,
            "alertaId": ev["alertaId"],
            "estacion": ev["estacion"],
            "nivel": ev["nivel"],
            "totalNotificados": len(destinatarios),
            "productores": destinatarios,
            "origen": "notificaciones",
            "timestamp": int(time.time() * 1000),
        }
        producer.send(KAFKA_TOPIC_ALERTAS, key=ev["estacion"], value=notif).get(timeout=10)
        print(json.dumps({**base, "eventType": tipo, "alertaId": ev["alertaId"], "nivel": ev["nivel"],
                          "rafaga": ev.get("rafaga"), "notificados": len(destinatarios), "status": "consumed"}))
    elif tipo == "alerta.notificada":
        print(json.dumps({**base, "eventType": tipo, "alertaId": ev.get("alertaId"),
                          "totalNotificados": ev.get("totalNotificados"), "status": "consumed"}))
    else:
        print(json.dumps({**base, "eventType": tipo, "status": "invalid", "error": "tipoEvento desconocido"}))
