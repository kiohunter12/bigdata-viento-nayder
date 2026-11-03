# Producto U2 — Pipeline en tiempo real de la dimensión **Viento** (Nayder Arce Mayta)

> Valores ⟨…⟩: completar con tus corridas. Toda captura con reloj y usuario visibles.

## 0. Producto
Pipeline en tiempo real que ingiere **eventos IoT reales** (viento actual de Open-Meteo en Juliaca, Puno
y Lampa) y **eventos de negocio** (suscripciones y alertas a productores agrícolas) por Kafka, los procesa
con Spark Structured Streaming (ventanas, watermark, checkpoint, dedup), expone métricas operativas en
Prometheus/Grafana con estimación de costos, y predice **la velocidad del viento dentro de 1 hora** en vivo.

**Pregunta U2:** ¿qué viento habrá en la próxima hora en Juliaca y cuándo supera el umbral de alerta (p90 de la U1)?

**Evolución desde la U1:** el umbral p90 y la climatología vienen del Gold de la U1; el histórico horario
se reutiliza para entrenar el modelo de S10; la misma API pasa de `archive` a `forecast current`.

## 1. Alcance por sesión
| Sesión | Capacidad | Implementación propia |
|---|---|---|
| S6 | Eventos de negocio | `suscripcion.creada`, `alerta.emitida`, `alerta.notificada` en `alerta-eventos` |
| S7 | IoT real | Open-Meteo → MQTT → bridge → `viento-eventos` (3 particiones, key sensorId), validación esquema/rango/alerta |
| S8 | Streaming | ventana tumbling 1 min, deslizante 5/1 min, watermark 2 min, tardío descartado, dedup (sensorId, obsTime), checkpoint |
| S9 | Observabilidad y costo | kafka-exporter + métricas Spark por micro-lote al Pushgateway, 21 paneles, 6 alertas, plantilla de costos |
| S10 | Serie de tiempo | velocidad t+1h · persistencia vs LR vs RF · split cronológico · inferencia en vivo |
| S11 | BI/ML | KPIs del flujo + KPIs U1 + real vs predicción en Grafana + MAE en producción |
| S12 | Integración | demo en vivo (abajo) |

## 2. Rúbrica U2 (A=20, B=15, C=10, D=5)
| # | Criterio | Peso | Mi evidencia |
|---|---|---|---|
| 1 | Ingesta de eventos de negocio | 12 % | logs `producer-suscripciones`, `alertador`, `notificaciones`; malformado sin caída; lag antes/después |
| 2 | Ingesta IoT | 10 % | Kafka UI 3 particiones; logs `forwarded`/`consumed`/`alerta`/`invalid`/`fuera_rango` |
| 3 | Spark Structured Streaming | 14 % | notebook 08: ventanas, watermark, tardío, dedup, checkpoint/offsets |
| 4 | Observabilidad | 12 % | Grafana fila 1–2; 3 targets UP; prueba de exporter caído |
| 5 | Costos y escalado | 8 % | notebook 09 §7: ⟨US$ …/mes⟩, ×10 y ×100 |
| 6 | Salidas BI/ML distribuidas | 12 % | Grafana fila 3–4; Pushgateway |
| 7 | Serie de tiempo/inferencia validada | 12 % | tabla S10 vs persistencia; MAE producción ⟨…⟩ |
| 8 | Sustentación | 20 % | demo + preguntas |

## 3. Resultados (completar)
### Ingesta (S6–S7) — evidencias del 02-nov-2026
- Broker MQTT **propio** (Mosquitto en Docker): el público `test.mosquitto.org:1883` daba *timeout* desde la red local → decisión propia, el pipeline no depende de un servicio externo.
- Lecturas reales: Juliaca 6.7 km/h (ráfaga 23.4), Puno 9.9 (22.7), Lampa 3.5 (8.6) → `published` → `forwarded` (Juliaca p0, Lampa p1, Puno p2) → `consumed` con **latencia 51 ms**.
- Negocio: 3 `suscripcion.creada` (P-001..P-003) consumidas; JSON roto → `invalid` sin caída; prueba ráfaga 62 km/h → `alerta` NARANJA → `alerta.emitida` ALR-6e6cd4ae → notificaciones (1 notificado, P-001) → `alerta.notificada`.
- Validación IoT: `invalid` (malformado), `fuera_rango` (humedad 140 %), `alerta` (ráfaga ≥ 46).
- Lag: consumidor detenido + 300 mensajes → **lag 303** (`EMPTY`, 0 miembros) → consumidor relanzado → **lag 0** (`STABLE`).
- Kafka UI: `viento-eventos` 3 particiones; la partición 2 tiene más mensajes porque la key de prueba `prueba-viento-99` cae en ella (misma key → misma partición).

### Streaming (notebook `08_streaming_viento_v2`)
- Checkpoint: al reiniciar el notebook, Spark **retomó** desde `offsets/22 → {"0":126,"1":128,"2":428}` y procesó lo pendiente (303 filas) → tolerancia a fallos demostrada.
- Tumbling 1 min: sin watermark (`complete`) re-emite todas las ventanas (18 filas); con watermark 2 min (`update`) solo las que cambian (12).
- Deslizante 5 min / 1 min: cada lectura cae en 5 ventanas.
- Evento tardío (10 min): **con watermark 0 filas** (descartado) vs **sin watermark 3 filas** (contado).
- Dedup `(sensorId, obsTime)`: `viento-duplicado` publicado 2 veces → 1; lecturas reales: 6 = 3 sensores × 2 pasos de 15 min, aunque llegaron cada 15 s.
- Trigger 2 s vs 10 s: más micro-lotes vs menos lotes; la latencia queda acotada por el trigger.

### Observabilidad y costos (notebook `09`)
- PromQL reales: `kafka_brokers`=1, `up{job="kafka-exporter"}`=1, targets UP = 3, lag `uso-viento-group`=0 y `alertador-viento-group`=0, **11.4 msg/min** en `viento-eventos`, latencia Spark 154 ms, ratio válidos 1.0.
- `lastProgress`: 3 filas/lote, 0.3 filas/s de entrada, 1.4 filas/s procesadas, `triggerExecution` ≈ 2.2 s, `avgOffsetsBehindLatest` = 0.
- Latencia publicación → procesado: **p50 2 870 ms, p95 8 503 ms** → alerta "latencia sensible" en **17 000 ms** (p95 × 2). Oscila 0–9 s porque está acotada por el trigger de 10 s.
- Alerta probada: `docker stop lambda26-kafka-exporter` → panel "Exporter arriba" = 0 (rojo) → `docker start` → 1.
- Edad del dato: subió hasta 14.7 min y bajó a 10 s a las 20:15 (nuevo paso de Open-Meteo) → alerta "dato viejo" a 30 min.
- Costo: pico 12 ev/min (0.2 ev/s); 3 × t3.medium × 730 h = **US$ 91.10/mes** + S3 ≈ US$ 0.09 → **≈ US$ 91.20/mes** sin red. El crecimiento medido (≈ 4 GB/mes) está **inflado por archivos pequeños** (cada micro-lote escribe un Parquet de pocos KB) → justifica compactar por día.
- ×10 estaciones → 2 ev/s; ×100 → 20 ev/s: Kafka sigue ocioso; lo primero que se escala es Spark (CPU) y la compactación del almacenamiento.

### Series de tiempo y BI (completar)
- S10 (velocidad dentro de 1 h, 147 455 horas, split cronológico train 117 964 hasta 2023-06-17 08:00 / test 29 491 desde 09:00):

  | Modelo | RMSE | MAE | R² | Mejora RMSE vs persistencia |
  |---|---|---|---|---|
  | Persistencia (v(t+1h) = v(t)) | 3.7468 | 2.5818 | 0.5273 | 0 % |
  | LinearRegression | 3.3269 | 2.3410 | 0.6273 | +11.21 % |
  | **RandomForest (50 árboles, prof. 8)** | **3.1649** | **2.2492** | **0.6627** | **+15.53 %** |

  Ganador RandomForest → `PipelineModel` (VectorAssembler + RF) en `artifacts/modelo_viento_1h`, recargado y aplicado en vivo:
  20:15 → predicción para 21:15: Juliaca 5.2 → 5.69 km/h, Lampa 7.6 → 7.52, Puno 4.3 → 7.23 (ráfaga 29.2 empuja la predicción hacia arriba).
  Las mismas features en batch y en streaming (`features()`) evitan *training/serving skew*.
- S11 KPIs publicados por PUT al Pushgateway:
  - Flujo (`lambda26_bi_flujo/viento_batch`): **144 eventos**, **100 % válidos**, latencia promedio **4 982 ms**, 6 lecturas únicas (sensor × paso de 15 min).
  - Batch U1 (`lambda26_bi_batch/u1_gold`, noviembre): ráfaga máxima media **38.6 km/h**, probabilidad de día fuerte **23.5 %**, velocidad media 8.8 km/h, umbral **46 km/h**.
  - En vivo (`lambda26_ml_streaming`, instance = sensorId): real vs predicción 1 h — Juliaca 5.2 → 5.69, Lampa 7.6 → 7.52, Puno 4.3 → 7.23.
  - Prueba de "en vivo": dos PromQL a 30 s → `push_time_seconds` cambió (…072.75 → …092.61) aunque el valor del viento siguió igual (Open-Meteo cambia cada 15 min).
  - Grafana: fila 3 (KPIs U1 batch + flujo) junto a fila 4 (ML en vivo) → batch y streaming lado a lado.
  - **Auditoría en producción** (02-nov, 21:15–23:15, 27 pares predicción a T vs real a T + 1 h):

    | Estación | n | MAE modelo | MAE persistencia | ¿Gana el modelo? |
    |---|---|---|---|---|
    | Juliaca | 9 | 3.52 | 3.94 | sí (−11 %) |
    | Puno | 9 | 3.23 | 3.76 | sí (−14 %) |
    | Lampa | 9 | 4.19 | 4.06 | **no** (+3 %) |
    | **Total** | **27** | **3.65** | **3.92** | **sí (−7 %)** |

  - Lectura honesta: el modelo mejora a la persistencia en producción, pero **menos que en el test** (MAE test 2.25 → producción 3.65). Causas: (1) entrenado solo con ERA5 de Juliaca y aplicado a Puno y Lampa; (2) en vivo recibe el pronóstico `current` (otro modelo, no ERA5); (3) en Lampa, de noche, el viento real cae a 0.3–0.7 km/h y el modelo predice 4–5 km/h → **sesgo hacia arriba en calma**. Muestra pequeña (3 h): conclusión preliminar.
  - Mejora propuesta: reentrenar por estación (o con `sensorId` como feature) y con datos de pronóstico histórico en vez de ERA5.
  - **Problema de archivos pequeños visto en vivo:** tras ~3 h de streaming con trigger de 10 s, `viento_eventos_observabilidad` tenía **2 172 archivos** y `predicciones_stream_s11` **682** → la lectura falló por falta de memoria (`Cannot allocate memory`). Se resolvió compactando (`coalesce(1)`): 2 172 → 1 y 682 → 1. En producción: compactación programada (p. ej. cada hora o por día) o trigger más largo.

## 4. Secuencia de demo S12 (3 min individuales)
1. (20 s) Grafana *Viento Juliaca — Pipeline U2*: fila 3 = U1 (batch), fila 4 = U2 (vivo) **lado a lado**.
2. (40 s) Terminal T3 publica lecturas reales → T2 `forwarded` → T1 `consumed`/`alerta`; Kafka UI mensajes por partición.
3. (20 s) `producer_suscripciones.py` → `notificaciones` lo consume; si hay alerta, `alerta.notificada`.
4. (40 s) Notebook 08: ventana con watermark corriendo + carpeta `offsets/` del checkpoint.
5. (30 s) Grafana filas 1–2: lag, throughput, latencia; costo en notebook 09.
6. (30 s) Panel real vs predicción + tabla vs persistencia.
7. Decisión distinta al ejemplo del docente: **la fuente real repite lecturas** (polling 60 s vs modelo 15 min) → dedup por `(sensorId, obsTime)` en Spark e idempotencia en el alertador.

## 5. Preguntas de referencia (respuestas cortas)
1. **Contrato negocio vs IoT:** IoT es continuo, por dispositivo (key sensorId, varias particiones, validación de rango); negocio es esporádico, por entidad (key estación, orden estricto, reglas de negocio). Por eso van en topics separados.
2. **Watermark y evento tardío:** con watermark de 2 min, una lectura con 10 min de atraso cae en una ventana ya cerrada y se descarta; sin watermark Spark guardaría estado para siempre (memoria crece) y la contaría.
3. **Métrica que avisa backpressure primero:** `triggerExecution` > intervalo del trigger y el `avgOffsetsBehindLatest`/lag creciendo — antes de que suba la latencia de punta a punta.
4. **Costo streaming vs batch:** streaming paga 24/7 aunque llegue poco; batch paga solo las horas que corre. Se dimensiona por pico, no por promedio.
5. **Predicción junto a KPIs:** el usuario ve "qué pasa" y "qué va a pasar" en el mismo lugar → decide antes (cubrir cultivos, suspender trabajos).
6. **Comparar con persistencia:** el viento de la próxima hora se parece mucho al actual; un R² alto no prueba nada si la persistencia ya lo logra. Si no la supera, el modelo no aporta y se dice.
7. **KPI de negocio vs métrica de infraestructura; Pushgateway:** "% días de ráfaga fuerte" o "alertas emitidas" vs "lag" o "latencia". Pushgateway es un atajo porque congela el último valor si el job muere.
