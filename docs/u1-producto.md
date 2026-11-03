# Producto U1 — Pipeline batch de la dimensión **Viento** (Nayder Arce Mayta)

> Los valores marcados ⟨…⟩ salen de **tu** corrida de `01_viento_batch_nayder.ipynb` (Restart & Run All). No inventar números.

## 0. Producto
Pipeline ETL distribuido en PySpark que toma 16 años de datos horarios de Open-Meteo (Juliaca), los
limpia, los agrega a nivel diario y genera una salida **Gold en Parquet particionada por año**, lista
para BI (climatología del viento) y ML (regresión de la ráfaga máxima diaria).

**Preguntas:** (1) ¿en qué meses/horas y desde dónde sopla más fuerte el viento en Juliaca?
(2) ¿qué tan probable es un día de ráfaga fuerte por mes? (3) ¿se puede estimar la ráfaga máxima de hoy con los datos de ayer?

## 1. Arquitectura Big Data — Lambda
Diagrama y justificación: sección S1 del notebook y README. Resumen: histórico grande que se recalcula
completo (batch) + flujo vivo liviano (speed, U2) que **reutiliza** el umbral p90 y el modelo del batch.

## 2. Transformaciones distribuidas con PySpark
| Paso | Evidencia en el notebook |
|---|---|
| Extracción con esquema explícito (`StructType`, 8 columnas) | sección S2 · **147 432** filas horarias (2010-01-01 → 2026-10-26) |
| Transformaciones: `to_timestamp`, `when` (Beaufort), sector de 8 rumbos, componentes u/v | 2.1 |
| Evaluación perezosa medida (transformación vs acción) | 2.2 · definir 0.078 s vs ejecutar `collect()` 1.19 s |
| `groupBy` + `agg` → diario | 2.3 |
| Plan de ejecución `explain(True)`: poda de columnas, HashAggregate parcial → Exchange(fecha, 8) → final | 2.4 |
| RDD `filter/map/reduceByKey` (horas con ráfaga ≥ 40 por año) | 2.5 |

## 3. Calidad de datos y particionamiento
| Control | Resultado |
|---|---|
| Nulos por columna | 0 en las 8 columnas |
| Duplicados de `fecha_hora` | 0 (147 432 = 147 432 distintas) |
| Fuera de rango físico (vel 0–200, ráfaga 0–250, dir 0–360, hum 0–100, presión 500–1100) | 0 |
| Días completos (24 h) en Silver | 6 143 (assert sin fechas duplicadas) |
| Gold `partitionBy("anio")` | 17 particiones (2010–2026) |
| Lectura de vuelta = escritura | assert OK |
| Poda de particiones | `PartitionFilters: [isnotnull(anio), (anio = 2024)]` → 366 filas de 2024 |

**Umbral de ráfaga fuerte** = p90 de la ráfaga máxima diaria = **46 km/h** (p50 32.0, p75 39.2, p90 46.4).

### 3.1 Funciones ventana y joins (particionado y performance)
| Técnica | Resultado |
|---|---|
| Ventana con partición `partitionBy("fecha")` + `row_number` | la ráfaga máxima del día ocurre sobre todo a las 17 h (1 095 días), 16 h (869) y 15 h (760); plan: `Exchange hashpartitioning(fecha)` → `Window` |
| Ventana móvil 7 días (`rowsBetween(-6, 0)`) sin partición | resultado en **1 partición** → válido para 6 mil días, no escala a millones; últimos 7 días ≈ 47–49 km/h (por encima del umbral) |
| Broadcast join climatología mensual (12 filas) × Gold | `BroadcastHashJoin`; día más anómalo: 2023-01-19 (65.5 km/h, z = 4.83); ráfaga más alta: 2026-08-17 (81.7 km/h, z = 4.24) |
| Sort-Merge vs Broadcast (horario 147 mil × diario 6 mil) | mismo resultado (7 074 filas; > 6 143 días porque hay empates de hora con la misma ráfaga máxima): **SMJ 0.88 s vs BHJ 0.66 s** (−25 %) |

### 3.2 Formato
CSV Bronze 6.97 MB → Gold Parquet 0.688 MB → Gold ORC 0.456 MB; consulta de 2024: Parquet 0.99 s, ORC 1.02 s.
Se mantiene **Parquet** (estándar de Spark/BI, poda de particiones y pushdown); ORC comprime algo más, pero no aporta para este volumen; Avro es por filas (sirve para mensajes Kafka, no para análisis).

## 4. Componente ML distribuido (regresión, MLlib)
Target: `rafaga_max` del día. Split **cronológico** 80/20 (train 4 913 días hasta 2023-06-15, test 1 229 días desde 2023-06-16).

| Modelo | Features | RMSE | MAE | R² | Mejora RMSE vs persistencia |
|---|---|---|---|---|---|
| Baseline persistencia (ayer = hoy) | — | 9.4474 | 7.0723 | 0.0026 | 0 % |
| A · LinearRegression | dia_sin, dia_cos | 10.6079 | 8.3124 | −0.2575 | −12.28 % |
| **B · LinearRegression** | A + 6 variables de ayer | **9.1518** | **7.0624** | **0.0640** | **+3.13 %** |
| C · RandomForest (50, d8, seed 42) | igual que B | 9.4876 | 7.3208 | −0.0059 | −0.43 % |

Ganador: **B · LR estacional + ayer** (único que supera a la persistencia, por poco), guardado como `PipelineModel` en `artifacts/modelo_rafaga_u1` y recargado para predecir.

### 4b. Clasificación complementaria: ¿día de ráfaga fuerte (≥ 46 km/h)?
Misma partición cronológica; pesos de clase (peso positivo 11.25). Días fuertes: **8.2 % en train vs 21.7 % en test** (el periodo 2023–2026 fue mucho más ventoso → *drift*).

| Modelo | AUC-ROC | AUC-PR | Precisión | Recall | F1 | VP | FP | FN | VN |
|---|---|---|---|---|---|---|---|---|---|
| Persistencia (ayer fuerte → hoy fuerte) | 0.7545 | 0.4672 | 0.464 | 0.464 | 0.464 | 124 | 143 | 143 | 819 |
| **LogReg (pesos)** | **0.7745** | **0.4735** | 0.408 | **0.674** | **0.509** | 180 | 261 | 87 | 701 |
| RF clasificador (pesos) | 0.7502 | 0.4284 | 0.450 | 0.521 | 0.483 | 139 | 170 | 128 | 792 |

Ganador: **LogReg con pesos** (`artifacts/modelo_dia_fuerte_u1`), curva ROC en `figs/u1_curva_roc.png`.
Lectura: detecta **2 de cada 3 días fuertes** (recall 67 % vs 46 % de la persistencia) a costa de más falsas alarmas (4 de cada 10 alertas aciertan). Para un productor agrícola conviene más **no perderse** un día de viento fuerte que recibir una alerta de más → se prioriza recall.

## 5. Respuesta a las preguntas
- **Meses con más días de ráfaga ≥ 46 km/h:** setiembre–noviembre (22–23.5 % de los días) y agosto (18.6 %); los más calmos son marzo–abril (≈ 1 %).
- **Hora pico:** 17:00 (velocidad media 12.3 km/h, ráfaga media 28.3 km/h); mínimo a las 08:00 (5.0 km/h). El viento se arma con el calentamiento de la tarde.
- **Rumbo dominante:** suroeste (22.1 %) y oeste (19.1 %) → 41 % del viento ≥ 5 km/h viene del SO/O.
- **Coeficientes Config B:** lo que más empuja la ráfaga de hoy es la velocidad media de ayer (+0.85 por km/h) y la ráfaga de ayer (+0.27); más humedad ayer → menos ráfaga hoy (−0.085 por %). El intercepto −184 compensa el término de presión (+0.32 × ~645 hPa).
- **Hallazgo honesto:** la ráfaga máxima diaria es muy ruidosa: ni la persistencia (R² ≈ 0) ni los modelos explican mucho (R² 0.06). La estacionalidad sola empeora (A < persistencia) porque el periodo de test (jun 2023 → oct 2026) fue más ventoso que el histórico de entrenamiento.
- **Hallazgo extra (RDD 2.5):** las horas con ráfaga ≥ 40 km/h suben de 63–174 por año (2010–2016) a 420–545 (2024–2025). Puede ser tendencia real o un cambio en ERA5; queda como pregunta abierta, no como conclusión.

## 6b. Rúbrica del aula virtual (7 criterios × 14 %) → evidencia
| Criterio | Dónde |
|---|---|
| Verifica la salida con controles de calidad y evidencia de ejecución reproducible | S3 (nulos, duplicados, rangos, asserts) + Restart & Run All con descarga automática |
| Configura almacenamiento distribuido y elige formatos (Parquet/Avro/ORC) con particionado adecuado | 3.2 Gold `partitionBy(anio)` + PartitionFilters + 3.4 Parquet vs ORC vs Avro |
| Distingue Lambda/Kappa y selecciona enfoque batch/streaming | sección S1 |
| Aplica joins y funciones ventana considerando particionado y performance | 3.3 (ventana con/sin partición, broadcast vs sort-merge) |
| Implementa transformaciones distribuidas con DataFrames y evidencia lazy evaluation / planes | 2.1–2.4 (lazy 0.078 s vs 1.19 s, `explain(True)`) |
| Aplica los fundamentos de Spark | SparkSession, esquema explícito, RDD `reduceByKey` (2.5), particiones, MLlib |
| Sustentación | en clase: demo del notebook + preguntas |

## 6. Rúbrica U1 (A=20, B=15, C=10, D=5) y dónde está la evidencia
| # | Criterio | Peso | Evidencia |
|---|---|---|---|
| 1 | Arquitectura seleccionada y justificada | 12 % | S1 del notebook |
| 2 | PySpark: extracción, transformación, agregación, distribuido | 16 % | 2.1–2.5 + explain |
| 3 | Particionado en formato analítico | 16 % | 3.2 + lectura de vuelta + PartitionFilters |
| 4 | Pipeline reproducible | 12 % | Restart & Run All, descarga automática desde la API |
| 5 | ML con ≥ 3 métricas, alternativa y guardado | 16 % | sección S4 |
| 6 | Evidencias con reloj y usuario | 8 % | capturas |
| 7 | Sustentación | 20 % | guion |

## 7. Preguntas de sustentación S5 (respuestas cortas)
1. **¿Kappa o Lambda?** Lambda: el histórico se recalcula completo cuando cambia una regla y alimenta a la capa speed (umbral y modelo).
2. **Transformación vs acción / explain:** `filter/groupBy` solo arman el plan; `count/collect/write` lo ejecutan. En el explain se ve el shuffle (`Exchange`) por `fecha`.
3. **Duplicados no resueltos:** contarían dos veces la misma hora → sesgan el promedio diario y rompen el assert de 24 h.
4. **Particionar por alta cardinalidad (p. ej. fecha):** miles de carpetas con archivos diminutos → más metadatos que datos; por eso `anio`.
5. **Guardar el modelo equivocado / sin VectorAssembler:** se guarda el `Pipeline` completo para que en producción se apliquen las mismas features.
