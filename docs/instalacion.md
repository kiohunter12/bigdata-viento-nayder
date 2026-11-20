# Instalación y ejecución

## Requisitos
- Windows 10/11 con **Docker Desktop** (motor en estado *Engine running*) y PowerShell.
- Git, y conexión a internet (Open-Meteo, Maven para el paquete `spark-sql-kafka` y Telegram).
- Un bot de Telegram (token de @BotFather).

## Configuración
1. Clonar: `git clone https://github.com/kiohunter12/bigdata-viento-nayder.git`
2. Copiar `uso-viento/.env.example` a `uso-viento/.env` y poner `TELEGRAM_TOKEN=<token>`. El archivo `.env` está en `.gitignore` y no se sube.
3. Opcional: `GRAFANA_ADMIN_USER` y `GRAFANA_ADMIN_PASSWORD` (por defecto admin/admin, solo demo local).

## Arranque
```powershell
cd "D:\c9\acuña\viento"
powershell -ExecutionPolicy Bypass -File scripts\levantar_todo.ps1
```
El script levanta Kafka, Mosquitto, Prometheus, Pushgateway, Grafana y Jupyter con `docker compose up -d --wait`; cada servicio tiene *healthcheck* y los dependientes esperan con `depends_on: condition: service_healthy`. También crea los topics `viento-eventos` (3 particiones) y `alerta-eventos` (1).

## Servicios
| Servicio | URL |
|---|---|
| Jupyter | http://localhost:4488 (token `sintoken`) |
| Kafka UI | http://localhost:48085 |
| Prometheus | http://localhost:49090 |
| Pushgateway | http://localhost:49091 |
| Grafana | http://localhost:43000 → Dashboards → Viento Juliaca |
