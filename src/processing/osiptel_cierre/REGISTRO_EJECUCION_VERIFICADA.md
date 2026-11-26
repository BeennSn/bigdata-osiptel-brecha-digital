# Registro final de ejecución — OSIPTEL OCI

**Fuente de evidencia:** log de ejecución compartido por el usuario, `Texto pegado(20261126-020139).txt`. La ejecución ocurrió el 26 de noviembre de 2026 alrededor de las 02:00 UTC (25 de noviembre, hora de Perú).

**Resultado:** `ESTADO_FINAL: COMPLETADO | codigo: 0`; `==== CIERRE DOCUMENTAL PUBLICADO Y VERIFICADO ====`.

**Versión inmutable publicada:** `v1_17b651ac372a96fb18fa`.

**Raíz:** `hdfs://namenode:8020/data/mart_documental_curado/`.

| Tabla publicada | Filas | Verificación |
| --- | ---: | --- |
| f2_menciones_documentales_verificadas | 287 | PUBLICADO_Y_VERIFICADO |
| f3_cifras_contextualizadas_verificadas | 12 | PUBLICADO_Y_VERIFICADO |
| f4_hogares_internet_narrativa_verificados | 20 | PUBLICADO_Y_VERIFICADO |
| f4_series_pendientes_revision | 74 | PUBLICADO_Y_VERIFICADO |
| f4_conflictos_excluidos | 102 | PUBLICADO_Y_VERIFICADO |
| comparacion_nacional_f1_f4_verificada | 5 | PUBLICADO_Y_VERIFICADO |

Controles: 71 períodos Gold F1, 287 F2, 12 F3, 20 F4 verificados, 74 F4 pendientes, 102 conflictos; 4 coincidencias temporales en la comparación nacional de 5 trimestres.

**Correcciones incluidas en esta copia para GitHub:**
1. Ampliación del timeout de preparación HDFS de 90 a 360 segundos en `ejecutar_cierre.py`.
2. Lectura Spark de los CSV locales mediante `path.as_uri()` (`file:///`) en `publicar_curados_spark.py`.

**Límites:** se certificó publicación técnica e integridad de los snapshots, no aprobación formal de OSIPTEL/INEI ni validez semántica de los 74 pendientes y 102 conflictos. La interfaz carga nuevos archivos a cuarentena, pero no automatiza su ingesta y revisión.
