---
hide:
  - navigation
  - toc
---

<div class="ca-hero" markdown>

# ClimateRisk Andes · Viento

Pipeline Big Data **Lambda** para anticipar el viento fuerte en el Altiplano de Juliaca.<br>
**Efrain Nayder Arce Mayta** · Big Data 2026-2 · Universidad Peruana Unión

<div class="ca-pregunta" markdown>
:material-help-circle-outline: **Pregunta central** — ¿Cuándo y con qué intensidad sopla el viento fuerte en el Altiplano de Juliaca, y cuánto viento habrá en la próxima hora para avisar a tiempo a los productores?
</div>

[Ver el brief :material-arrow-right:](brief.md){ .md-button .md-button--primary }
[Repositorio :fontawesome-brands-github:](https://github.com/kiohunter12/bigdata-viento-nayder){ .md-button }

</div>

## Resultados en una mirada

<div class="ca-kpis">
  <div class="ca-kpi"><div class="v">147 432</div><div class="l">horas de histórico procesadas (2010 → 2026)</div></div>
  <div class="ca-kpi"><div class="v">46 km/h</div><div class="l">umbral de ráfaga fuerte (p90)</div></div>
  <div class="ca-kpi"><div class="v">+15.5 %</div><div class="l">mejora del modelo a 1 h sobre la persistencia</div></div>
  <div class="ca-kpi"><div class="v">3.65</div><div class="l">MAE en producción (km/h) vs 3.92 de persistencia</div></div>
  <div class="ca-kpi"><div class="v">100 %</div><div class="l">eventos válidos en el stream</div></div>
  <div class="ca-kpi"><div class="v">≈ US$ 91</div><div class="l">costo mensual estimado en la nube</div></div>
</div>

## Arquitectura Lambda

```mermaid
flowchart LR
  subgraph B["Capa batch · U1"]
    A1["Open-Meteo<br/>archive ERA5"] --> A2["CSV horario<br/>Bronze"]
    A2 --> A3["PySpark<br/>Silver diario"]
    A3 --> A4[("Gold Parquet<br/>partitionBy anio")]
    A4 --> A5["MLlib<br/>ráfaga · día fuerte"]
  end
  subgraph S["Capa speed · U2"]
    C1["Open-Meteo<br/>current · 15 min"] --> C2["MQTT<br/>Mosquitto"]
    C2 --> C3["Bridge"] --> C4[["Kafka<br/>viento-eventos"]]
    C4 --> C5["Spark Structured<br/>Streaming"]
    C4 --> C6["Alertador"] --> C7[["Kafka<br/>alerta-eventos"]]
    C7 --> C8["Notificaciones"]
  end
  A4 -. "umbral p90" .-> C6
  A5 -. "modelo +1 h" .-> C5
  C5 --> G["Prometheus · Pushgateway<br/>Grafana"]
  C4 --> G
```

## Lo que entrega el proyecto

<div class="grid cards" markdown>

-   :material-database-cog:{ .lg .middle } **Producto U1 · Batch**

    ---

    ETL distribuido con PySpark, calidad de datos, Parquet particionado, joins y ventanas, regresión y clasificación con MLlib.

    [:octicons-arrow-right-24: Ver producto U1](u1-producto.md)

-   :material-lightning-bolt:{ .lg .middle } **Producto U2 · Streaming**

    ---

    Kafka con eventos IoT y de negocio, Spark Structured Streaming, observabilidad, costos y predicción en vivo.

    [:octicons-arrow-right-24: Ver producto U2](u2-producto.md)

-   :material-file-document-check:{ .lg .middle } **Brief**

    ---

    Pregunta central, dimensiones, indicadores, fuentes y requisitos del sistema.

    [:octicons-arrow-right-24: Ver brief](brief.md)

-   :material-swap-horizontal:{ .lg .middle } **Contratos de eventos**

    ---

    Esquemas versionados de `viento.lectura`, `suscripcion.creada`, `alerta.emitida` y `alerta.notificada`.

    [:octicons-arrow-right-24: Ver contratos](contratos.md)

</div>

## Hallazgos principales

=== "Climatología (U1)"

    ![Climatología del viento](img/u1_climatologia_viento.png)

    - Más días de ráfaga fuerte entre **setiembre y noviembre** (22–23.5 %); marzo y abril son los más calmos.
    - El viento se arma con el calentamiento de la tarde: **pico a las 17:00**.
    - El **41 %** del viento sopla desde el **suroeste y oeste**.

=== "Modelos (U1)"

    ![Curva ROC](img/u1_curva_roc.png)

    - Clasificación de día fuerte: regresión logística con pesos, **AUC 0.775** y **recall 67 %** (persistencia: 46 %).
    - *Drift*: en el entrenamiento solo el 8 % de los días eran fuertes y en el test, el 22 %.

=== "Tiempo real (U2)"

    | Modelo · velocidad +1 h | RMSE | MAE | R² |
    |---|---|---|---|
    | Persistencia | 3.75 | 2.58 | 0.53 |
    | Regresión lineal | 3.33 | 2.34 | 0.63 |
    | **Random forest** | **3.16** | **2.25** | **0.66** |

    En producción el modelo gana a la persistencia en Juliaca y Puno; en Lampa sobrestima las noches de calma.

=== "Operación (U2)"

    - Latencia publicación → procesado: **p50 2.9 s · p95 8.5 s** (acotada por el trigger de 10 s).
    - Tolerancia a fallos demostrada con checkpoint, watermark y deduplicación.
    - Problema de archivos pequeños visto en vivo (**2 172 → 1** al compactar).
