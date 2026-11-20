"""Servicio de notificaciones (consumidor + productor de negocio, S6).

Lee `alerta-eventos`:
  suscripcion.creada -> guarda el suscriptor por estación
  alerta.emitida     -> regla determinista: cuenta suscriptores de esa estación
                        y publica el TERCER evento propio `alerta.notificada`
  alerta.notificada  -> solo se registra
  cualquier otra cosa o JSON roto -> status "invalid", el consumidor NO se cae

Observabilidad (S9): al arrancar relee TODO el topic (sin consumer group) para
reconstruir suscriptores y contadores, y en cada evento publica en el
Pushgateway las métricas `viento_negocio_*` que muestra la fila 5 de Grafana.
"""
import json
import os
import time

import requests
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from comun import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTAS, UMBRAL_RAFAGA_KMH

GROUP_ID = "notificaciones-group"
PUSHGATEWAY = os.getenv("PUSHGATEWAY", "http://lambda26-pushgateway:9091")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHATS_FILE = "/app/telegram_chats.json"


def chats_telegram():
    """Personas que tocaron INICIAR en el bot (se guardan en un archivo)."""
    try:
        chats = set(json.load(open(CHATS_FILE)))
    except Exception:
        chats = set()
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=10).json()
        for u in r.get("result", []):
            m = u.get("message") or {}
            if "chat" in m:
                chats.add(m["chat"]["id"])
        json.dump(sorted(chats), open(CHATS_FILE, "w"))
    except Exception as ex:
        print(json.dumps({"component": "telegram", "status": "error", "error": str(ex)}))
    return chats


def avisar_telegram(ev):
    if not TELEGRAM_TOKEN:
        return 0
    texto = (f"⚠️ ALERTA {ev['nivel']} de viento en {ev['estacion']}\n"
             f"Ráfaga: {ev.get('rafaga')} km/h (umbral {UMBRAL_RAFAGA_KMH:.0f})\n"
             f"Toma tus precauciones. · Wayra Alerta")
    n = 0
    for chat in chats_telegram():
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": chat, "text": texto}, timeout=10)
            n += 1
        except Exception:
            pass
    print(json.dumps({"component": "telegram", "alertaId": ev.get("alertaId"), "enviados": n, "status": "sent"}))
    return n

suscriptores = {}   # estacion -> set(productorId)
emitidas = {}       # (estacion, nivel) -> n
ultima = {}         # estacion -> (rafaga, ts_seg)
cont = {"notificadas": 0, "productores": 0}


def contar(ev):
    """Actualiza el estado de negocio con un evento (sin producir nada)."""
    tipo = ev.get("tipoEvento")
    if tipo == "suscripcion.creada":
        suscriptores.setdefault(ev["estacion"], set()).add(ev["productorId"])
    elif tipo == "alerta.emitida":
        k = (ev["estacion"], ev["nivel"])
        emitidas[k] = emitidas.get(k, 0) + 1
        ultima[ev["estacion"]] = (float(ev.get("rafaga", 0)), ev.get("timestamp", time.time() * 1000) / 1000)
    elif tipo == "alerta.notificada":
        cont["notificadas"] += 1
        cont["productores"] += int(ev.get("totalNotificados", 0))


def push_metricas():
    l = ["# TYPE viento_negocio_alertas_emitidas_total counter"]
    for (est, niv), n in sorted(emitidas.items()):
        l.append(f'viento_negocio_alertas_emitidas_total{{estacion="{est}",nivel="{niv}"}} {n}')
    if not emitidas:
        l.append('viento_negocio_alertas_emitidas_total{estacion="ninguna",nivel="ninguno"} 0')
    l.append("# TYPE viento_negocio_alertas_notificadas_total counter")
    l.append(f'viento_negocio_alertas_notificadas_total {cont["notificadas"]}')
    l.append("# TYPE viento_negocio_productores_notificados_total counter")
    l.append(f'viento_negocio_productores_notificados_total {cont["productores"]}')
    l.append("# TYPE viento_negocio_suscriptores gauge")
    for est, s in sorted(suscriptores.items()):
        l.append(f'viento_negocio_suscriptores{{estacion="{est}"}} {len(s)}')
    l.append("# TYPE viento_negocio_ultima_alerta_rafaga_kmh gauge")
    l.append("# TYPE viento_negocio_ultima_alerta_timestamp_seconds gauge")
    for est, (raf, ts) in sorted(ultima.items()):
        l.append(f'viento_negocio_ultima_alerta_rafaga_kmh{{estacion="{est}"}} {raf}')
        l.append(f'viento_negocio_ultima_alerta_timestamp_seconds{{estacion="{est}"}} {ts}')
    l.append("# TYPE viento_negocio_umbral_kmh gauge")
    l.append(f"viento_negocio_umbral_kmh {UMBRAL_RAFAGA_KMH}")
    try:
        requests.put(f"{PUSHGATEWAY}/metrics/job/negocio_alertas", data="\n".join(l) + "\n", timeout=3)
    except requests.RequestException as ex:
        print(json.dumps({"service": "uso-viento", "component": "notificaciones",
                          "status": "push_error", "error": str(ex)}))


# 1) Reconstruir el estado releyendo el topic completo (no confirma offsets)
replay = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=None,
                       enable_auto_commit=False, consumer_timeout_ms=4000)
parts = [TopicPartition(KAFKA_TOPIC_ALERTAS, p) for p in (replay.partitions_for_topic(KAFKA_TOPIC_ALERTAS) or [])]
replay.assign(parts)
replay.seek_to_beginning(*parts)
fin_replay = {tp.partition: off for tp, off in replay.end_offsets(parts).items()}
for m in replay:
    try:
        contar(json.loads(m.value.decode("utf-8")))
    except Exception:
        pass
replay.close()
push_metricas()

# 2) Servicio normal con consumer group
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

print(json.dumps({"service": "uso-viento", "component": "notificaciones", "topic": KAFKA_TOPIC_ALERTAS,
                  "groupId": GROUP_ID, "suscriptores": {k: sorted(v) for k, v in suscriptores.items()},
                  "alertasHistoricas": sum(emitidas.values()), "status": "listening"}))

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

    if msg.offset >= fin_replay.get(msg.partition, 0):  # no contar dos veces lo ya releído
        contar(ev)

    if tipo == "suscripcion.creada":
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
        if msg.offset >= fin_replay.get(msg.partition, 0):
            avisar_telegram(ev)
        print(json.dumps({**base, "eventType": tipo, "alertaId": ev["alertaId"], "nivel": ev["nivel"],
                          "rafaga": ev.get("rafaga"), "notificados": len(destinatarios), "status": "consumed"}))
    elif tipo == "alerta.notificada":
        print(json.dumps({**base, "eventType": tipo, "alertaId": ev.get("alertaId"),
                          "totalNotificados": ev.get("totalNotificados"), "status": "consumed"}))
    else:
        print(json.dumps({**base, "eventType": tipo, "status": "invalid", "error": "tipoEvento desconocido"}))
    push_metricas()
