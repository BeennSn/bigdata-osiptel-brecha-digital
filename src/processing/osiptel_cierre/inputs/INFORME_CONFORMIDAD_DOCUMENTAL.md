# Revisión documental reproducida del conjunto OSIPTEL / INEI (da.zip)

**Alcance:** comprobación técnica y semántica de las extracciones originales, no certificación institucional. No se modificaron los archivos Bronze, Silver ni Gold en OCI. La revisión se realizó sobre el ZIP proporcionado; las recomendaciones se materializan en CSV adjuntos.

## Inventario de archivos originales

- **Fuente 1:** 1 XLSX de conexiones de internet fijo. SHA-256: `e6e88de8b58ab84f25159d63584300e453d9bb7fe5057f5f31976a602f2ca29a`. Coincide con el origen del snapshot Silver F1 publicado en OCI (`v1_e6e88de8b58ab84f2515`). No se revalidaron manualmente las 141.106 filas aquí; se dispone de la conciliación previa de HDFS.
- **Fuente 2:** 142 PDF; se reprodujeron 287 menciones en 88 documentos mediante el extractor documentado. Se verificaron SHA-256, páginas existentes y presencia de las expresiones regulatorias en el texto original para **287/287** (278 mediante PyMuPDF y 9 mediante pdftotext debido a las diferencias de extracción textual).
- **Fuente 3:** 103 PDF; se reprodujeron exactamente **12/12** cifras y se contrastaron cada una con su PDF y página original, diferenciando valores de stock, variaciones y subconjuntos de fibra óptica u operadores.
- **Fuente 4:** 89 PDF; se reprodujeron **408** evidencias en 44 documentos, sin errores de lectura; 45 PDF no aportaron registros al extractor conservador. Se reprodujeron **94 grupos sin discrepancia** y **102 con discrepancia**. Todos los 408 registros conservan hash de PDF y referencia a página existente.

## Conformidad por fuente

| Fuente | Conforme para entrega técnica | Apto para indicador cuantitativo definitivo | Observación |
|---|---|---|---|
| F1 | Sí, conciliación e integridad del archivo | Sí, en los Gold F1 previamente publicados | No sumar stocks entre meses |
| F2 | Sí: 287 menciones documentales localizadas | **No como mandatos aprobados** | 287 menciones no representan 287 expedientes únicos ni concesiones confirmadas. El archivo adjunto indica exactamente qué se identificó. |
| F3 | Sí: 12 valores contrastados y semánticamente reetiquetados | Sí, cuando se utilice cada indicador con la dimensión, periodo y naturaleza indicados | Solo 2 son *totales nacionales* de stock: 3.540.474 para 2023-12 y 4.274.935 para 2025-09. Dos registros son stock de fibra, uno es stock de grupo de operadores, otros son variaciones. |
| F4 | Sí: datos extraídos y conflictos separados | **20 valores narrativos** con evidencia inequívoca, en 5 trimestres × 4 ámbitos. Otros 74 grupos sin conflicto requieren cotejo semántico de las tablas; 102 conflictos siguen excluidos. | Comparar hogares con acceso a Internet (INEI) versus conexiones fijas (OSIPTEL) solo como indicadores paralelos, no como medidas equivalentes. |

## Fuente 3: interpretación corregida

1. Huancavelica 47,95 %, Ica 30,66 %, Apurímac 22,62 %, Lambayeque 18,12 %: variación interanual de **conexiones fijas** a junio de 2022 (frente a junio de 2021), no tasas generales de penetración.
2. 332.829 conexiones a marzo de 2023: **grupo WI-NET y Optical Technologies**, no total nacional.
3. 244.206 conexiones a junio de 2023: **disminución de Telefónica frente a junio de 2022**, no stock.
4. 3.540.474 conexiones a diciembre de 2023: **total nacional de internet fijo**.
5. 2.540.374 conexiones a junio de 2024: **fibra óptica nacional**, no total de todas las tecnologías.
6. 2.783.379 conexiones a septiembre de 2024: **fibra óptica nacional**.
7. 153.363 conexiones a marzo de 2025: **disminución de Telefónica respecto de marzo de 2024**, no stock.
8. 4.274.935 conexiones a septiembre de 2025: **total nacional**.
9. 167.551 conexiones a septiembre de 2025: **pérdida interanual de Integratel**, no stock.

## Fuente 4: periodo reciente cotejado directamente

| Trimestre | Nacional (%) | Lima Metropolitana (%) | Resto urbano (%) | Área rural (%) | PDF original |
|---|---:|---:|---:|---:|---|
| 2025-T1 | 58,9 | 80,3 | 60,7 | 20,5 | `informe-tecnico_tecnologiasdelainformacion_ene_feb_mar2025.pdf`, p. 6 |
| 2025-T3 | 59,7 | 78,7 | 62,8 | 23,6 | `tic-iii-trimestre_2025_1.pdf`, p. 6 |
| 2025-T4 | 60,1 | 78,2 | 63,1 | 23,4 | `boletin-tic-oct_dic2025.pdf`, p. 6 |
| 2026-T1 | 62,7 | 82,7 | 65,0 | 25,0 | `boletin_tic_itrim2026.pdf`, p. 6 |
| 2026-T2 | 64,5 | 84,7 | 66,2 | 27,1 | `boletin-tic-iit2026.pdf`, p. 6 |

Para los **20 valores** se cotejaron el trimestre, ámbito y porcentaje en el párrafo explícito de la página 6. 2026-T2 no tiene correspondencia temporal con el Gold F1 vigente, que llega a 2026-03; debe permanecer como indicador INEI separado.

## Ejemplos verificados de conflictos reales en la F4

- **2014-T2, Nacional:** el boletín de 2014 (p. 5) informa 25,6 % y la tabla comparativa del boletín de 2015 (p. 6) informa 25,9 % para 2014-T2. Ambos valores aparecen literalmente en los documentos y corresponden a columnas del mismo periodo. Sin documentación adicional, no se debe escoger arbitrariamente uno.
- **2014-T3, Lima Metropolitana:** boletín de 2014 (p. 5) 41,2 %, frente a boletín de 2015 (p. 6) 44,8 % para ese periodo. Ambos requieren decisión documentada respecto de revisiones de datos.

Es incorrecto asumir que los 102 conflictos son errores de OCR o que una extracción sin discrepancias ya garantiza la metodología y comparabilidad. La documentación disponible no permite certificar una serie completa depurada de los 49 trimestres nacionales sin reglas de revisión de boletines históricos.

## Qué puede utilizar Axel ahora

- Gold de F1: nacional mensual y territorial histórico, con filtros por fecha y dimensiones. Ya publicado y verificado en HDFS de OCI.
- F2: **catálogo de menciones regulatorias** auditable, no indicador del número de mandatos concedidos ni de infraestructura construida.
- F3: archivo `F3_12_cifras_corregidas_y_verificadas.csv`; cada registro tiene periodo, ámbito, interpretación, fuente y página. Para comparar con F1, separar siempre stock nacional, fibra, operador y diferencia interanual.
- F4: `F4_20_indicadores_narrativa_verificados.csv` con cinco trimestres y cuatro ámbitos. Integrar con F1 únicamente en nivel nacional y cuando exista cierre del mismo trimestre. Las diferencias entre porcentajes de hogares y números absolutos de conexiones no constituyen una medición causal de brecha digital.
- F4 histórica: conservar `F4_94_series_con_estado_revision.csv` y `F4_102_conflictos_para_revision.csv` como pistas auditables, **no todos aprobados**.

## Limitaciones y autoridad

Esta es una conformidad técnica-documental asistida, basada en originales proporcionados. No equivale a una firma del responsable de Calidad o del profesor. Una aprobación formal de datos regulatorios, o una curaduría estadística completa de todas las series históricas, exige verificar la condición jurídica de las resoluciones y los cambios de metodología o revisiones oficiales. **La auditoría no aplicó cambios sobre HDFS en OCI ni transforma los actuales productos `mart_preliminar` en Gold aprobado.** Los archivos entregados son la base para una publicación posterior controlada.
