# Pruebas y evidencias

## Métricas verificadas
| Prueba | Resultado |
|---|---|
| Regresión batch (ráfaga de hoy con datos de ayer) | RMSE 9.15 (config B) vs 9.45 persistencia |
| Clasificación de día fuerte | AUC 0.775 vs 0.755; recall 67 % |
| Matriz de confusión (test) | VP 180 · FP 261 · FN 87 · VN 701 |
| Modelo +1 h (offline) | RF RMSE 3.16 vs 3.75 persistencia (+15.5 %), R² 0.66 |
| Modelo +1 h (producción, 27 pares) | MAE 3.65 vs 3.92; Grafana 3.67 vs 3.95 |
| Latencia extremo a extremo | p50 2.9 s · p95 8.5 s |
| Deduplicación | 2 634 eventos → 55 únicas |
| Lag de consumidores | 303 → 0 |
| Costo estimado | US$ 91.11 al mes |

## Verificación de arranque (20/11/2026)
Con `levantar_todo.ps1`, Kafka, Kafka UI, kafka-exporter, Mosquitto, Pushgateway, Prometheus y Grafana quedaron en estado **Healthy**. En Grafana (fila 1) el throughput de `viento-eventos` fue de 1 a 3 mensajes por minuto y los tres *consumer groups* estuvieron con lag 0.

## Caso de error
Mensaje con JSON inválido enviado con `publicar_prueba.py`: falla `validar_esquema` (`comun.py`) y se descarta sin detener el flujo.

!!! warning "Pendiente"
    Ejecución integrada con compuertas y manifiesto `manifiesto_<runId>.json`, y prueba de no regresión antes/después del hardening.
