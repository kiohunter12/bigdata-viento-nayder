# Levanta todo el stack en orden. Ejecutar desde la carpeta raiz del proyecto (viento\)
#   powershell -ExecutionPolicy Bypass -File scripts\levantar_todo.ps1
$ErrorActionPreference = "Stop"
docker compose -f kafka/compose.yml up -d
Start-Sleep -Seconds 10
powershell -ExecutionPolicy Bypass -File scripts/crear_topics.ps1
docker compose -f uso-viento/compose.yml up -d --build
docker compose -f obs/compose.yml up -d
if (-not (Test-Path pyspark/.env)) { Copy-Item pyspark/.env.example pyspark/.env }
docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d --build
Write-Host ""
Write-Host "Jupyter     http://localhost:4488  (token: sintoken)"
Write-Host "Kafka UI    http://localhost:48085"
Write-Host "Prometheus  http://localhost:49090"
Write-Host "Pushgateway http://localhost:49091"
Write-Host "Grafana     http://localhost:43000  (admin/admin) -> Dashboards -> Viento Juliaca"
