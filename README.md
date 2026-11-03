# ClimateRisk Andes · Dimensión **Viento** (Juliaca) — Nayder Arce Mayta

Big Data 2026-2 · UPeU · Arquitectura **Lambda** sobre la API **Open-Meteo**
(histórico ERA5 para la U1, viento actual cada 15 min para la U2).

```
U1 batch   archive-api ──► CSV ──► PySpark (Bronze→Silver→Gold Parquet/anio) ──► MLlib ráfaga de hoy
U2 speed   forecast-api current ──► MQTT (Mosquitto propio) ──► bridge ──► Kafka viento-eventos ──► Spark Streaming
                                                         │        │                 (ventanas, watermark,
                                                         │        └► alertador ─► alerta-eventos ◄─ suscripciones
                                                         │                                      └► notificaciones
                                                         └► kafka-exporter ─► Prometheus ◄─ Pushgateway ◄─ Spark (S9/S11)
                                                                                   └────► Grafana "Viento Juliaca"
```

## Estructura

| Carpeta | Qué es | Sesiones |
|---|---|---|
| `pyspark/` | Jupyter + PySpark 4.2.0 (igual al curso + matplotlib/requests) | todas |
| `pyspark/sesiones/u1-viento/01_viento_batch_nayder.ipynb` | Producto U1 completo | S1–S4 → S5 |
| `kafka/` | Kafka 4.3.1 + Kafka UI + kafka-exporter (del curso) | S6–S11 |
| `uso-viento/` | ingesta Python: productor Open-Meteo, bridge MQTT, consumidores, eventos de negocio | S6–S7 |
| `pyspark/sesiones/u2-viento/` | `08_streaming_viento_v2`, `09_observabilidad_viento_nayder`, `10_series_tiempo_viento_nayder`, `11_bi_ml_viento_v2` | S8–S11 → S12 |
| `obs/` | Prometheus + Pushgateway + Grafana con el tablero ya provisionado | S9, S11 |
| `docs/` | contratos de eventos, `u1-producto.md`, `u2-producto.md` | S5, S12 |

## 0. Levantar todo (PowerShell, en esta carpeta)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\levantar_todo.ps1
```
Jupyter http://localhost:4488 (token `sintoken`) · Kafka UI http://localhost:48085 ·
Prometheus http://localhost:49090 · Grafana http://localhost:43000 (admin/admin).

> Si ya tienes contenedores `lambda26-*` del curso corriendo desde otra carpeta, bájalos primero
> (mismo nombre de contenedor = conflicto): `docker ps -a --filter name=lambda26`.

## U1 — S1 a S5 (batch)

1. Abre `u1-viento/01_viento_batch_nayder.ipynb` → **Kernel ▸ Restart & Run All**.
   La primera celda descarga 2010→hoy desde Open-Meteo (~1 min; luego se reutiliza el CSV).
2. Anota el **UMBRAL p90** que imprime la sección 3.1 y ponlo en `uso-viento/compose.yml`
   (`UMBRAL_RAFAGA_KMH`), luego `docker compose -f uso-viento/compose.yml up -d`.
3. Salidas: `sesiones/artifacts/viento_gold/anio=*/`, `modelo_rafaga_u1/`, `metricas_u1.json`, `figs/*.png`.

## U2 — S6 a S12 (tiempo real)

Abre **4 terminales** (cada una con su log, para las capturas):

```powershell
# T1 — consumidor IoT (correr primero)
docker compose -f uso-viento/compose.yml exec uso-viento python consumer_viento.py
# T2 — puente MQTT → Kafka
docker compose -f uso-viento/compose.yml exec uso-viento python bridge_mqtt_kafka.py
# T3 — productor REAL (Open-Meteo → MQTT). Para demos de ventanas usa -e INTERVALO_S=15
docker compose -f uso-viento/compose.yml exec uso-viento python productor_openmeteo.py
#      el broker MQTT es el contenedor lambda26-mosquitto (test.mosquitto.org daba timeout)
# T4 — negocio: notificaciones + alertador
docker compose -f uso-viento/compose.yml exec uso-viento python consumer_alertas.py
docker compose -f uso-viento/compose.yml exec uso-viento python alertador_viento.py
```

| Sesión | Qué correr | Evidencia mínima |
|---|---|---|
| **S6** negocio | `python producer_suscripciones.py` · `python publicar_prueba.py viento_fuerte` (alerta completa) · `python publicar_prueba.py alerta_negocio` (malformado) · `publicar_prueba.py rafaga 300` con el consumer detenido (lag sube/baja en Kafka UI → Consumers) | Kafka UI con 2 topics, logs de productor/consumidor, contrato `alerta.notificada`, mensaje malformado sin caída |
| **S7** IoT real | T1–T3 + `publicar_prueba.py fuera_rango` | Kafka UI `viento-eventos` 3 particiones, logs `forwarded`/`consumed`/`alerta` con datos reales, `invalid`/`fuera_rango` |
| **S8** streaming | `u2-viento/08_streaming_viento_v2.ipynb` (Run All, ~8 min) | ventanas tumbling/deslizante, tardío descartado, dedup, carpeta de checkpoint |
| **S9** observabilidad | `09_observabilidad_viento_nayder.ipynb` + Grafana | `lastProgress`, 3 targets UP, panel lag, latencia por minuto, alertas, costos |
| **S10** series | `10_series_tiempo_viento_nayder.ipynb` | tabla vs persistencia, split cronológico, modelo guardado, predicciones en vivo |
| **S11** BI/ML | `11_bi_ml_viento_v2.ipynb` (Run All; la celda 5 de auditoría se corre a mano ≥ 75 min después) | KPIs en Pushgateway, 2 PromQL distintas, tablero con real vs predicción |
| **S12** | todo arriba + tablero *Viento Juliaca — Pipeline U2* | ver `docs/u2-producto.md` (secuencia de demo) |

**Capturas:** siempre con el reloj de Windows y tu usuario visibles, y con commits del mismo día en GitHub.

## Fuente de datos — lo que hay que decir con honestidad
- `archive-api` = reanálisis ERA5 (~25 km); `forecast` `current` = modelo de pronóstico actualizado cada
  15 min (en Perú es interpolado de datos horarios). **No** es un anemómetro en Juliaca.
- Se consulta cada 60 s pero el valor cambia cada 15 min → hay lecturas repetidas: se deduplican por
  `(sensorId, obsTime)` en Spark y el alertador es idempotente. Es una decisión propia distinta al ejemplo del docente.
- Límite gratuito de Open-Meteo: 10 000 llamadas/día. Con 3 puntos en 1 llamada cada 15 s → 5 760/día: OK.
