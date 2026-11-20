# Uso del sistema

## Flujo principal (U2, en vivo)
Una terminal por proceso, desde la carpeta del proyecto:

```powershell
docker compose -f uso-viento/compose.yml exec uso-viento python bridge_mqtt_kafka.py      # 1. puente MQTT -> Kafka
docker compose -f uso-viento/compose.yml exec uso-viento python productor_openmeteo.py    # 2. productor (3 estaciones, cada 60 s)
docker compose -f uso-viento/compose.yml exec uso-viento python consumer_viento.py        # 3. consumidor de viento
docker compose -f uso-viento/compose.yml exec uso-viento python alertador_viento.py       # 4. alertador (umbral 46 km/h)
docker compose -f uso-viento/compose.yml exec uso-viento python consumer_alertas.py       # 5. notificador Telegram
```

## Notebooks (Jupyter)
| Notebook | Para qué sirve |
|---|---|
| `01_viento_batch_nayder` (U1) | Descarga, Bronze/Silver/Gold, umbral 46, modelos |
| `08_streaming_viento_v2` | Ventanas, watermark, dedup, checkpoint |
| `09_observabilidad_viento_nayder` | Métricas al Pushgateway, latencia, costos |
| `10_series_tiempo_viento_nayder` | Entrenamiento del modelo +1 h |
| `11_bi_ml_viento_v2` | KPIs batch y del flujo, inferencia en vivo, auditoría |

## Cómo probarlo
1. Abrir Kafka UI → `viento-eventos` y ver llegar mensajes de las 3 estaciones.
2. Abrir Grafana → *Viento Juliaca*.
3. Enviar `/start` al bot `@wayra_alerta_juliaca_bot`; al superar 46 km/h de ráfaga llega la alerta.
4. **Caso de error:** publicar un JSON roto con `publicar_prueba.py`; el consumidor lo descarta por validación de esquema y lo cuenta como inválido.
