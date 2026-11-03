# Brief técnico-analítico — ClimateRisk Andes · Dimensión Viento

> Declara por escrito el sistema Big Data que se construye en el ciclo. Se actualiza solo si cambia el alcance.
> **Cambio de alcance (nov-2026):** la dimensión pasa de *precipitación* a **viento**; la lluvia en vivo valía 0 casi todo el tiempo en época seca y no daba señal en streaming.

## 1. Datos del equipo

| Campo | Valor |
|---|---|
| Nombre del equipo | — (trabajo individual) |
| Sección | ⟨completar⟩ |
| Repositorio | <https://github.com/kiohunter12/bigdata-viento-nayder> |
| Topics configurados | sí (`bigdata`, `campus-juliaca`, `semestre-2026-2`, `linea-cdia`, `tipo-ps`, sección y grupo) |

| Integrante | Rol o énfasis |
|---|---|
| Efrain Nayder Arce Mayta | todo el pipeline: batch/Spark (U1), streaming/Kafka, observabilidad y BI/ML (U2) |

## 2. Dominio del proyecto

- **Proyecto:** ClimateRisk Andes.
- **Problema:** en el Altiplano (Juliaca, Puno, Lampa) el viento fuerte daña cultivos, coberturas y la elaboración de chuño, y los productores no tienen un aviso local y oportuno.
- **Dominio de datos:** meteorología; velocidad, dirección y ráfaga del viento a 10 m, más temperatura, humedad, presión y nubosidad.
- **Pregunta central de negocio:** *¿Cuándo y con qué intensidad sopla el viento fuerte en el Altiplano de Juliaca, y cuánto viento habrá en la próxima hora para avisar a tiempo a los productores?*
- **Usuarios:** productores agrícolas suscritos (reciben alertas), analista de riesgo (tablero Grafana), sistema automático de alertas.
- **Arquitectura:** **Lambda**. El histórico de 16 años se recalcula completo (batch) y alimenta la capa en tiempo real con el umbral p90 y los modelos entrenados.
- **Fuente batch:** API Open-Meteo *Historical Weather* (reanálisis ERA5), Juliaca (-15.50082, -70.13980), horario desde 2010.
- **Fuente en tiempo real:** API Open-Meteo *Forecast* `current` (cada 15 min) para Juliaca, Puno y Lampa, publicada por MQTT → Kafka; más eventos de negocio (`suscripcion.creada`, `alerta.emitida`, `alerta.notificada`).
- **Nota:** es el mismo origen (Open-Meteo) visto en dos momentos: histórico acumulado y lectura en vivo.
- **Continuidad:** continuación del proyecto ClimateRisk Andes de la U1 (2026-2), con cambio de dimensión.

## 3. Dimensiones de análisis

| Integrante | Tipo | Dimensión | Indicador | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Nayder Arce | Predictiva U1 / batch | ¿En qué meses y horas hay ráfagas fuertes y cuál será la ráfaga máxima de hoy? | % de días con ráfaga ≥ p90 (46 km/h) por mes; ráfaga máxima diaria predicha; probabilidad de día fuerte | Open-Meteo archive (ERA5) horario 2010→hoy | no aplica |
| Nayder Arce | Predictiva U2 / streaming | ¿Cuánto viento habrá en la próxima hora y cuándo supera el umbral de alerta? | velocidad real vs predicha a +1 h por estación; alertas por ráfaga ≥ 46 km/h; MAE en producción | histórico ERA5 (entrenamiento) | Open-Meteo `current` → MQTT → Kafka `viento-eventos` |

### Dimensión U1: climatología y ráfaga diaria (integrante: Nayder Arce)
- **Tipo:** predictiva sin tiempo real (U1, batch).
- **Dimensión:** ¿en qué meses, horas y desde qué dirección sopla más fuerte el viento, y se puede estimar la ráfaga máxima de hoy con lo que pasó ayer? Responde al "cuándo y con qué intensidad" de la pregunta central.
- **Indicadores:** % de días con ráfaga ≥ 46 km/h por mes; hora pico; rumbo dominante; ráfaga máxima diaria predicha (regresión) y probabilidad de día fuerte (clasificación).
- **Decisión:** planificar labores sensibles (coberturas, secado de chuño, fumigación) en los meses y horas de menos viento.
- **Fuente batch:** Open-Meteo archive (`fecha_hora`, `temperatura`, `humedad`, `presion`, `nubosidad`, `velocidad`, `direccion`, `rafaga`), 147 432 horas.
- **Fuente streaming:** no aplica.
- **Modelo:** regresión (LinearRegression / RandomForest) y clasificación (LogisticRegression con pesos) en Spark MLlib, ejecutados una vez.
- **Salida:** notebook `01_viento_batch_nayder.ipynb` (tablas, gráficos, métricas, curva ROC) y Gold Parquet particionado por año.
- **Combinación en el tablero:** sus KPIs (ráfaga media del mes, probabilidad de día fuerte, umbral p90) aparecen en Grafana junto a los de U2.
- **Requisitos:**
  1. El sistema debe descargar y procesar el histórico de forma reproducible con PySpark.
  2. El sistema debe validar calidad (nulos, duplicados, rangos) y guardar Parquet particionado.
  3. El sistema debe calcular el umbral de ráfaga fuerte y entrenar, comparar y guardar un modelo predictivo.

### Dimensión U2: viento en la próxima hora y alertas (integrante: Nayder Arce)
- **Tipo:** predictiva con tiempo real (U2, streaming).
- **Dimensión:** ¿cuánto viento habrá en la próxima hora en cada estación y cuándo supera el umbral de alerta? Responde al "avisar a tiempo" de la pregunta central.
- **Indicadores:** velocidad real vs predicha a +1 h; alertas emitidas y notificadas; MAE en producción vs persistencia; latencia y validez del flujo.
- **Decisión:** avisar a los productores suscritos antes del viento fuerte.
- **Fuente batch:** histórico ERA5 horario (entrenamiento del modelo).
- **Fuente streaming:** `viento-eventos` (contrato `viento.lectura` v1) y `alerta-eventos` (eventos de negocio).
- **Modelo:** serie de tiempo (velocidad t+1 h) con RandomForest MLlib, aplicado en vivo sobre el stream.
- **Salida:** tablero común Grafana *Viento Juliaca — Pipeline U2* (KPIs, predicción en vivo, MAE).
- **Combinación:** comparte tablero con los KPIs batch de U1.
- **Requisitos:**
  1. El sistema debe ingerir lecturas reales y eventos de negocio por Kafka con contratos versionados.
  2. El sistema debe procesar el stream con ventanas, watermark, checkpoint y deduplicación.
  3. El sistema debe predecir en vivo, exponer métricas en Grafana y medir el error en producción.

**Qué SÍ cubre:** viento en 3 puntos del Altiplano, histórico 2010→hoy, alertas por umbral, predicción a 1 h.
**Qué NO cubre:** sensores físicos propios (la fuente es un modelo meteorológico, no un anemómetro), pronósticos a más de 1 h, envío real de SMS.

## 4. Aprobación
- Docente: Abel Angel Sullon Macalupu
- Fecha: ⟨completar⟩
