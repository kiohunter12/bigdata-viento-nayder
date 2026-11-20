# Módulos y funcionalidades

| Módulo | Archivo | Qué hace | Relación con el problema |
|---|---|---|---|
| Ingesta batch | notebook 01 | Descarga 147 456 horas de `/archive` | Base histórica para saber cuándo golpea el viento |
| Calidad y capas | notebook 01 | Bronze, Silver (6 144 días), Gold por año | Datos confiables para modelar |
| Umbral | notebook 01 | p90 de ráfaga diaria = 46.4, redondeado a 46 | Define qué es "viento fuerte" |
| Productor | `productor_openmeteo.py` | Una lectura por estación cada 60 s a MQTT | Dato en vivo |
| Puente | `bridge_mqtt_kafka.py` | MQTT → Kafka con clave `sensorId` | Desacopla y ordena |
| Validación | `comun.py` | Esquema, rangos físicos, nivel de alerta | No alertar con datos inválidos |
| Streaming | notebook 08 | Ventanas, watermark, dedup, checkpoint | Procesamiento tolerante a fallos |
| Alertador | `alertador_viento.py` | Ráfaga ≥ 46 genera alerta (con dedup) | La decisión: avisar o no |
| Notificador | `consumer_alertas.py` | Telegram solo para alertas nuevas | Llega al celular |
| Modelo +1 h | notebooks 10 y 11 | Random Forest que predice la ráfaga de la próxima hora | Anticipación |
| Observabilidad | notebooks 09 y 11, `obs/` | Pushgateway → Prometheus → Grafana (29 paneles) | Ver y auditar el sistema |
