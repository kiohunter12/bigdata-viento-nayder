"""Configuración y contrato compartidos por los scripts de uso-viento.

Contrato `viento.lectura` v1 (ver docs/contratos.md):
    tipoEvento, version, sensorId, estacion, lat, lon,
    velocidadViento (km/h), direccionViento (grados), rafaga (km/h),
    temperatura (°C), humedad (%), presion (hPa, presión en superficie),
    obsTime (epoch ms del paso de 15 min del modelo), origen,
    timestamp (epoch ms en que se publicó la lectura)
"""
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_VIENTO = os.getenv("KAFKA_TOPIC_VIENTO", "viento-eventos")
KAFKA_TOPIC_ALERTAS = os.getenv("KAFKA_TOPIC_ALERTAS", "alerta-eventos")

MQTT_HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/viento/nayder/lecturas")

INTERVALO_S = int(os.getenv("INTERVALO_S", "60"))
UMBRAL_RAFAGA_KMH = float(os.getenv("UMBRAL_RAFAGA_KMH", "40"))

# Tres "sensores" = tres puntos reales del Altiplano consultados en Open-Meteo.
# Juliaca usa las mismas coordenadas que el histórico de la Unidad I.
SENSORES = [
    {"sensorId": "juliaca-viento-01", "estacion": "Juliaca", "lat": -15.50082, "lon": -70.13980},
    {"sensorId": "puno-viento-02", "estacion": "Puno", "lat": -15.84020, "lon": -70.02190},
    {"sensorId": "lampa-viento-03", "estacion": "Lampa", "lat": -15.36360, "lon": -70.36610},
]

CAMPOS_OBLIGATORIOS = [
    "tipoEvento", "sensorId", "velocidadViento", "direccionViento",
    "rafaga", "temperatura", "humedad", "presion", "obsTime", "timestamp",
]

# Rango físicamente posible (fuera de esto = sensor/dato corrupto, NO clima).
# OJO: Juliaca está a ~3825 m, la presión en superficie ronda 640 hPa.
RANGO_FISICO = {
    "velocidadViento": (0.0, 200.0),
    "direccionViento": (0.0, 360.0),
    "rafaga": (0.0, 250.0),
    "temperatura": (-40.0, 50.0),
    "humedad": (0.0, 100.0),
    "presion": (500.0, 1100.0),
}


def validar_esquema(evento):
    """Devuelve la lista de campos faltantes (vacía = esquema OK)."""
    if not isinstance(evento, dict):
        return ["<no es objeto JSON>"]
    return [c for c in CAMPOS_OBLIGATORIOS if evento.get(c) is None]


def fuera_de_rango(evento):
    """Devuelve las variables fuera del rango físico."""
    malas = []
    for variable, (minimo, maximo) in RANGO_FISICO.items():
        valor = evento.get(variable)
        if not isinstance(valor, (int, float)) or not (minimo <= valor <= maximo):
            malas.append(variable)
    return malas


def nivel_alerta(rafaga):
    """Alerta operativa (umbral de negocio, no de rango físico)."""
    if rafaga >= UMBRAL_RAFAGA_KMH * 1.25:
        return "NARANJA"
    if rafaga >= UMBRAL_RAFAGA_KMH:
        return "AMARILLA"
    return None
