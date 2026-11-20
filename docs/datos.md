# Datos

## Fuente
API pública **Open-Meteo**, sin clave. Dos endpoints: `/archive` (histórico, U1) y `/forecast` con `current` (en vivo, U2, se actualiza cada 15 min; 3 estaciones en una llamada).

## Capas (U1)
| Capa | Contenido | Tamaño |
|---|---|---|
| Bronze | CSV crudo `viento_juliaca_horario.csv`, esquema explícito de 8 columnas | 147 456 horas (2010–2026) |
| Silver | Limpio (sin nulos, duplicados ni valores fuera de rango) y agregado por día | 6 144 días |
| Gold | Parquet particionado por año, con variables de ayer para ML | 17 particiones |

Variables: temperatura, humedad, presión, nubosidad, velocidad, dirección y ráfaga; derivadas `fecha`, `hora`, `mes`, `u`, `v`.

## Evento en vivo (U2)
Mensaje JSON por estación con `sensorId`, `obsTime` y las variables meteorológicas. Clave Kafka: `sensorId`. Contrato completo en [Contratos de eventos](contratos.md).

## Almacenamiento
- Parquet en `pyspark/sesiones/artifacts/` (Gold, modelos, métricas, `umbral_viento.json`).
- Kafka (retención local) para los eventos y checkpoints de Spark para el estado del streaming.
- Los CSV crudos no se versionan; el notebook los descarga desde la API.
