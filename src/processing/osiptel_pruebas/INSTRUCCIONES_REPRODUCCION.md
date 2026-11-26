# OSIPTEL: ejecución verificada y reproducción de la interfaz

## Estado verificado en OCI

- Repositorio: `BeennSn/bigdata-osiptel-brecha-digital`, rama `feature/oci-bronze`.
- Gold F1 histórico: `v2_b6e4155ae58e8f9106d8`.
- Mart documental curado F2/F3/F4: `v1_17b651ac372a96fb18fa`.
- Prueba integral en OCI: **6/6 OK**, estado `APROBADO_TECNICAMENTE`, reportado el 26/11/2026 UTC.
- Panel (solo túnel SSH): `http://127.0.0.1:8767`.
- Nuevos PDF: se evalúan, clasifican, publican manualmente en Bronze+Silver desde el botón A y, tras cotejo humano explícito, en Gold **documental independiente** desde el botón B.
- Prueba real: resolución OSIPTEL `000069-2026-CD`, SHA-256 `f2aec8a9bc395b1ea824a0fb7f2dff6f463ecad6ec68170c28fd4f5a19778759`.

## Reproducir en Oracle donde ya está instalado

```bash
# Acceso: entrar-oracle (desde WSL)
# En ubuntu@angeldev:
python3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py
```

Desde otra terminal WSL:

```bash
ssh -N -L 8767:127.0.0.1:8767 -i ~/ssh-key-2026-03-26.key ubuntu@158.101.36.142
```

El directorio de ejecución se mantiene en `/srv/osiptel/scripts/` y los scripts están publicados en `src/processing/osiptel_pruebas/` y `src/processing/osiptel_ingesta_pdf_v1/`. En despliegues nuevos, copiar ambos directorios de vuelta a `/srv/osiptel/scripts/` y preparar las dependencias HDFS/Docker; no se incluye el archivo de clave SSH ni los PDF.

## Arquitectura funcional

1. F1: Gold nacional y territorial basado en XLSX de OSIPTEL.
2. F2/F3: menciones y cifras extraídas de PDF, con procedencia documental y estados de revisión.
3. F4: 20 indicadores documentales verificados, 74 series pendientes y 102 conflictos fuera de indicadores aprobados.
4. Mart `v1_17b651ac372a96fb18fa`: seis productos publicados en HDFS.
5. Panel de decisiones: vuelve a leer HDFS por Spark para mostrar evolución F1/F4, prioridades territoriales, contexto de F2/F3, y descargar JSON de resultados.
6. PDF nuevo: cuarentena -> evaluar -> **A** original PDF en `/data/raw/pdf_controlados_v1/<sha>/` y candidatos en `/data/silver/pdf_nuevos_candidatos_v1/<sha>/v1` -> cotejo humano -> **B** registro en `/data/gold/pdf_documental_revisado_v1/<sha>/v1_<firma>`.

La segunda pulsación de B devolvió `EXISTENTE_VERIFICADO`: no generó una versión Gold distinta para la misma revisión.

## Límites que deben declararse en la sustentación

- La evaluación de PDF nuevos propone candidatos; no certifica validez jurídica ni aprueba automáticamente la semántica.
- El Gold documental nuevo no modifica los KPI oficiales de conexiones fijas ni integra automáticamente un nuevo periodo a los agregados nacionales.
- El componente de carga es **PDF controlado**, no un flujo genérico completo para XLSX, ZIP ni cualquier PDF escaneado.
- F1 mide cantidad de conexiones fijas; F4 mide porcentajes de hogares con cualquier tipo de acceso a internet. No son métricas intercambiables.
- El cambio de conexiones entre cierres no demuestra por sí mismo pérdida de cobertura territorial.
- El panel de pruebas permanece accesible solo mediante `127.0.0.1` y túnel SSH. No exponer el puerto 8767 públicamente ni utilizarlo como servicio multiusuario autenticado.
- El informe de pruebas 6/6 es una validación técnica de este entorno y muestras concretas, no una garantía universal de calidad de cada PDF futuro.

## Evidencia

Archivos `evidencia_validacion/REPORTE_6_DE_6.json`, `DECISIONES_HDFS.json`, `CIERRE_PRUEBA_PDF_BRONZE_SILVER_GOLD.md`. No se incluyen PDF originales, ficheros de cuarentena, credenciales ni logs con datos privados.
