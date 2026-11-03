"""Mensajes de PRUEBA controlada (marcados origen="prueba-manual").

Sirven para evidenciar que los consumidores no se caen y clasifican bien:
  python publicar_prueba.py malformado    -> JSON roto en viento-eventos       (status invalid)
  python publicar_prueba.py incompleto    -> falta 'rafaga'                    (status invalid)
  python publicar_prueba.py fuera_rango   -> humedad 140 %                     (status fuera_rango)
  python publicar_prueba.py alerta_negocio-> JSON roto en alerta-eventos       (notificaciones: invalid)
  python publicar_prueba.py rafaga N      -> 300 mensajes válidos rápido (para ver el LAG subir y bajar)
  python publicar_prueba.py viento_fuerte -> lectura válida de PRUEBA en Juliaca con ráfaga 62 km/h
                                             (para demostrar alerta.emitida -> alerta.notificada cuando no hay viento real fuerte)
"""
import json
import sys
import time

from kafka import KafkaProducer

from comun import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTAS, KAFKA_TOPIC_VIENTO

p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, key_serializer=lambda k: k.encode("utf-8"))
caso = sys.argv[1] if len(sys.argv) > 1 else "malformado"
ahora = int(time.time() * 1000)
base = {"tipoEvento": "viento.lectura", "version": 1, "sensorId": "prueba-viento-99", "estacion": "Prueba",
        "lat": -15.5, "lon": -70.1, "velocidadViento": 12.0, "direccionViento": 90.0, "rafaga": 25.0,
        "temperatura": 10.0, "humedad": 50.0, "presion": 640.0, "obsTime": ahora,
        "origen": "prueba-manual", "timestamp": ahora}

if caso == "malformado":
    p.send(KAFKA_TOPIC_VIENTO, key="prueba-viento-99", value=b'{"sensorId": "prueba-viento-99", "rafaga": ')
elif caso == "incompleto":
    ev = dict(base); ev.pop("rafaga")
    p.send(KAFKA_TOPIC_VIENTO, key="prueba-viento-99", value=json.dumps(ev).encode())
elif caso == "fuera_rango":
    ev = dict(base, humedad=140.0)
    p.send(KAFKA_TOPIC_VIENTO, key="prueba-viento-99", value=json.dumps(ev).encode())
elif caso == "alerta_negocio":
    p.send(KAFKA_TOPIC_ALERTAS, key="Prueba", value=b"esto no es json")
elif caso == "viento_fuerte":
    ev = dict(base, sensorId="prueba-viento-99", estacion="Juliaca", velocidadViento=38.0, rafaga=62.0)
    p.send(KAFKA_TOPIC_VIENTO, key="prueba-viento-99", value=json.dumps(ev).encode())
elif caso == "rafaga":
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    for i in range(n):
        ev = dict(base, obsTime=ahora + i, timestamp=int(time.time() * 1000))
        p.send(KAFKA_TOPIC_VIENTO, key="prueba-viento-99", value=json.dumps(ev).encode())
else:
    sys.exit(f"caso desconocido: {caso}")

p.flush()
print(json.dumps({"service": "uso-viento", "component": "prueba", "caso": caso, "status": "published"}))
