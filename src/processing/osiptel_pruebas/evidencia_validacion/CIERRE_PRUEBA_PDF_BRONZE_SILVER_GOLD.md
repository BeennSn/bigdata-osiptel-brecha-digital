# Evidencia de integración final: resolución de OSIPTEL

**Documento de ensayo:** Resolución N.° 000069-2026-CD/OSIPTEL.

**SHA-256:** `f2aec8a9bc395b1ea824a0fb7f2dff6f463ecad6ec68170c28fd4f5a19778759`.

## Evaluación de documento nuevo

- Estado: `PDF_NUEVO_ACTO_REGULATORIO_DETECTADO_PENDIENTE_COTEJO`.
- Familia: `F2`, 3 páginas.
- Candidato: artículo 1, página 3, acción detectada `DEROGAR`.
- No implica mandato de compartición de infraestructura ni expansión de cobertura.

## Ingesta real desde el panel (reportada por el operador)

- Estado final Bronze + Silver: `BRONZE_SILVER_PUBLICADOS`.
- HDFS Bronze: `hdfs://namenode:8020/data/raw/pdf_controlados_v1/f2aec8a9bc395b1ea824a0fb7f2dff6f463ecad6ec68170c28fd4f5a19778759/documento.pdf`.
- HDFS Silver: `hdfs://namenode:8020/data/silver/pdf_nuevos_candidatos_v1/f2aec8a9bc395b1ea824a0fb7f2dff6f463ecad6ec68170c28fd4f5a19778759/v1`.
- Candidatos detectados: 1.

## Registro de revisión y Gold documental

- Estado final: `GOLD_DOCUMENTAL_CON_REVISION_REGISTRADA`.
- HDFS Gold: `hdfs://namenode:8020/data/gold/pdf_documental_revisado_v1/f2aec8a9bc395b1ea824a0fb7f2dff6f463ecad6ec68170c28fd4f5a19778759/v1_f4c5b6f0233080b3d1c5`.
- Al pulsar B de nuevo, la salida reportó `gold_estado: EXISTENTE_VERIFICADO`, sin nueva versión.
- **Alcance:** conformidad de cotejo documental registrada por un revisor. No equivale a certificación institucional ni a validar vigencia de la norma. No modifica los KPI oficiales F1.

## Pruebas negativas realizadas

- PDF sobre adquisición de licencias de software: `FUERA_DE_ALCANCE_ANALITICO`, sin candidatos.
- PDF INEI previamente auditado: cuatro cifras corroboradas de 2026-T1, coincidencia SHA-256; no se volvió a publicar como documento nuevo.

## Estado técnico global previo

Archivo `REPORTE_6_DE_6.json` reporta `APROBADO_TECNICAMENTE` y seis controles OK: hash de CSV, `_SUCCESS` HDFS, Spark preflight, idempotencia de seis tablas, indicadores/decisiones reales e interfaz HTTP/CSRF/cuarentena.
