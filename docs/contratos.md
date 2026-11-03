# Contratos de eventos — dimensión Viento (Nayder)

Versionado: cada evento lleva `version` (entero). Cambio compatible (campo nuevo opcional) → misma versión;
cambio incompatible (renombrar/quitar campo, cambiar unidad) → `version + 1` y los consumidores aceptan ambas
durante la transición. Todos los `timestamp` son **epoch en milisegundos**.

## 1. Evento IoT `viento.lectura` v1

| Campo | Tipo | Unidad / regla |
|---|---|---|
| tipoEvento | string | `"viento.lectura"` |
| version | int | `1` |
| sensorId | string | **key de Kafka** (`juliaca-viento-01`, `puno-viento-02`, `lampa-viento-03`) |
| estacion | string | Juliaca / Puno / Lampa |
| lat, lon | double | grados |
| velocidadViento | double | km/h a 10 m — rango físico 0–200 |
| direccionViento | double | grados, de dónde viene — 0–360 |
| rafaga | double | km/h a 10 m — 0–250 |
| temperatura | double | °C — −40 a 50 |
| humedad | double | % — 0–100 |
| presion | double | hPa en superficie — 500–1100 (Juliaca ≈ 640) |
| obsTime | long | epoch ms del paso de 15 min del modelo Open-Meteo |
| origen | string | `open-meteo` (real) · `prueba-manual` (pruebas controladas) |
| timestamp | long | epoch ms en que se leyó/publicó |

- **Topic:** `viento-eventos` · 3 particiones (una por sensor) · RF 1
- **Productores:** `productor_openmeteo.py` → MQTT `lambda26/viento/nayder/lecturas` → `bridge_mqtt_kafka.py`
- **Consumidores:** `uso-viento-group` (consumer_viento.py), `alertador-viento-group` (alertador_viento.py), Spark (S8–S11)
- **Estados del consumidor:** `invalid` (esquema) · `fuera_rango` (rango físico) · `alerta` (ráfaga ≥ umbral p90) · `consumed`

```json
{"tipoEvento":"viento.lectura","version":1,"sensorId":"juliaca-viento-01","estacion":"Juliaca",
 "lat":-15.50082,"lon":-70.1398,"velocidadViento":18.4,"direccionViento":72,"rafaga":37.1,
 "temperatura":14.2,"humedad":38,"presion":641.3,"obsTime":1793647800000,
 "origen":"open-meteo","timestamp":1793648112345}
```

## 2. Eventos de negocio (topic `alerta-eventos`, 1 partición, key = `estacion`)

### `suscripcion.creada` v1
Un productor agrícola registra su parcela para recibir avisos. Productor: `producer_suscripciones.py`.
```json
{"tipoEvento":"suscripcion.creada","version":1,"suscripcionId":"SUS-1a2b3c4d","productorId":"P-001",
 "estacion":"Juliaca","canal":"SMS","contacto":"900000001","origen":"producer-suscripciones","timestamp":1793648000000}
```

### `alerta.emitida` v1
Lo publica `alertador_viento.py` cuando una lectura real supera el umbral. Idempotente por `(sensorId, obsTime)`.
Nivel: `AMARILLA` si ráfaga ≥ umbral, `NARANJA` si ≥ 1.25 × umbral.
```json
{"tipoEvento":"alerta.emitida","version":1,"alertaId":"ALR-9f8e7d6c","estacion":"Juliaca",
 "sensorId":"juliaca-viento-01","nivel":"AMARILLA","rafaga":47.5,"umbral":45,
 "obsTime":1793647800000,"origen":"alertador-viento","timestamp":1793648113000}
```

### `alerta.notificada` v1 (tercer evento propio)
Lo publica `consumer_alertas.py` (grupo `notificaciones-group`) con la regla determinista
"notificar a todos los suscritos de esa estación".
```json
{"tipoEvento":"alerta.notificada","version":1,"alertaId":"ALR-9f8e7d6c","estacion":"Juliaca",
 "nivel":"AMARILLA","totalNotificados":1,"productores":["P-001"],"origen":"notificaciones","timestamp":1793648114000}
```

## ¿Por qué dos topics distintos?
| | IoT `viento-eventos` | Negocio `alerta-eventos` |
|---|---|---|
| Quién lo dispara | un dispositivo/API, sin humano | una persona o una regla de negocio |
| Frecuencia | continua (cada 15–60 s × 3) | esporádica |
| Key | `sensorId` → orden por sensor, 1 partición c/u | `estacion` → orden de alertas por estación |
| Validación | rango físico + esquema | esquema + reglas de negocio |
| Duplicados | esperados (polling) → dedup en Spark | no deben existir → idempotencia en el alertador |
