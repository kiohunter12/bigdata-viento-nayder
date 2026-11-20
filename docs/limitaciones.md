# Limitaciones y mejoras

## Limitaciones
1. El histórico usa *Best Match* de Open-Meteo (tres regímenes de modelo), que puede sesgar el entrenamiento.
2. Los datos son de reanálisis y pronóstico, no de un anemómetro propio.
3. La mejora del modelo sobre la persistencia es modesta (RMSE 9.15 vs 9.45 en batch).
4. El alertador usa la ráfaga **observada**; no dispara con la predicción.
5. Falta el manifiesto de ejecución con compuertas y la prueba de no regresión formal.
6. Riesgo aceptado: Grafana con credenciales por defecto en entorno local de demo.

## Mejoras priorizadas
1. Manifiesto `manifiesto_<runId>.json` con compuertas automáticas.
2. Integración continua con GitHub Actions (pruebas y despliegue de MkDocs).
3. Alertar con la predicción a +1 h para dar aviso anticipado.
4. Gestor de secretos y rotación del token de Telegram.
5. Más estaciones y un sensor propio.
