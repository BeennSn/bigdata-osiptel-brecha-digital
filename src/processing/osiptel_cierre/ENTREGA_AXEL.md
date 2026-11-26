# Contrato de datos — Entrega a análisis (OSIPTEL)

**Origen:** OSIPTEL F1 y F3, regulación OSIPTEL F2, INEI F4. Procesamiento histórico validado y revisión documental reproducible. Entrega técnica, no visto bueno institucional.

## Tablas ya publicadas antes del cierre

- **F1 nacional**, 71 periodos mensuales: `hdfs://namenode:8020/data/gold/osiptel_historico_nacional_f1/v2_b6e4155ae58e8f9106d8`
- **F1 territorial**, 129.225 registros analíticos agrupados y 138.419 filas de origen: `hdfs://namenode:8020/data/gold/osiptel_historico_territorial_f1/v2_b6e4155ae58e8f9106d8`
- **Mart preliminar anterior**, cinco tablas ya verificadas: `hdfs://namenode:8020/data/mart_preliminar/<tabla>/v2_834f4750e6e80778966e` (contiene F2/F3/F4 **sin curar**).

## Nuevas tablas revisadas, publicadas y verificadas en Oracle

**Raíz HDFS publicada:** `hdfs://namenode:8020/data/mart_documental_curado/`

**Versión publicada:** `v1_17b651ac372a96fb18fa`.

| Tabla | Filas | Interpretación |
|---|---:|---|
| f2_menciones_documentales_verificadas | 287 | Catálogo de referencias regulatorias corroboradas, **no** número de mandatos adjudicados |
| f3_cifras_contextualizadas_verificadas | 12 | Variaciones, stocks generales, fibra y operadores, cada cifra con unidad, periodo y entidad |
| f4_hogares_internet_narrativa_verificados | 20 | Cobertura de hogares con Internet en 5 trimestres, ámbitos Nacional, Lima Metropolitana, resto urbano y rural |
| f4_series_pendientes_revision | 74 | No aptas para indicadores oficiales hasta revisión de la tabla original |
| f4_conflictos_excluidos | 102 | No aptas para indicadores definitivos; se excluyen del subconjunto validado |
| comparacion_nacional_f1_f4_verificada | 5 | Descriptiva nacional: 4 trimestres con ambos indicadores; 2026-T2 INEI sin F1 |

## Indicadores nacionales INEI cotejados directamente

| Periodo | % hogares con Internet |
|---|---:|
| 2025-T1 | 58,9 |
| 2025-T3 | 59,7 |
| 2025-T4 | 60,1 |
| 2026-T1 | 62,7 |
| 2026-T2 | 64,5 |

**Reglas fundamentales:** F1 `conexiones_fijas` es un *stock* de conexiones de internet fijo al cierre de un mes; no sumar meses como clientes distintos. F4 `% hogares con Internet` mide un concepto distinto, incluyendo otras formas de acceso; no restar números F1 y porcentajes F4 ni inferir una brecha departamental desde F4 nacional. F2 no representa proyectos ejecutados; F3 no permite sumar valores de diferentes naturalezas.

## Recomendación de consumo

Desde Spark con acceso a `namenode:8020`:

```python
base = 'hdfs://namenode:8020/data/mart_documental_curado'
ver = 'v1_17b651ac372a96fb18fa'
comparacion = spark.read.parquet(f'{base}/comparacion_nacional_f1_f4_verificada/{ver}')
comparacion.orderBy('periodo_trimestre').show(truncate=False)
```

**Estado de publicación:** el registro de Oracle del 26/11/2026 02:00 UTC confirma `ESTADO_FINAL: COMPLETADO | codigo: 0`, seis mensajes `PUBLICADO_Y_VERIFICADO` y verificaciones de contenido. No equivale a certificación institucional.
