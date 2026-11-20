# Repositorio y estándares

- **Repositorio:** https://github.com/kiohunter12/bigdata-viento-nayder
- **Documentación publicada:** https://kiohunter12.github.io/bigdata-viento-nayder/

## Estructura
```
pyspark/sesiones/   notebooks U1 y U2 + artifacts
uso-viento/app/     productor, puente, consumidores, alertador
kafka/  obs/        compose de Kafka y de Prometheus/Grafana
scripts/            levantar_todo.ps1, crear_topics.ps1
docs/  mkdocs.yml   documentación
```

## Estándares
- **Reproducibilidad:** todo se levanta con un comando (`levantar_todo.ps1`); versiones de imágenes fijadas en los compose.
- **Secretos:** `uso-viento/.env` ignorado por git; se entrega `.env.example`.
- **Salud de servicios:** healthchecks y `depends_on: service_healthy`.
- **Datos:** los CSV crudos no se versionan; se descargan desde la API.
- **Documentación:** MkDocs Material con las 10 secciones mínimas de la U3.
