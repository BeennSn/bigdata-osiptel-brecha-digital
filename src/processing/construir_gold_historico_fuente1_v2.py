#!/usr/bin/env python3
"""OSIPTEL Fuente 1: Gold historico nacional y territorial, version 2.

- Lee exclusivamente un snapshot Silver inmutable identificado por SOURCE_SHA.
- Reconcilia filas, duplicados exactos, vacios territoriales y periodos.
- Sustituye exceptAll (costoso) por firmas agregadas de SHA-256 por fila,
  comparadas antes/despues de escribir Parquet, mas conteos y sumas.
- Publica versiones nuevas e inmutables. No toca Gold anterior ni staging v1.
- El uso de una firma agregada no equivale a prueba formal de igualdad exacta;
  ofrece una comprobacion de integridad con riesgo de colision despreciable.

Ejecucion: /opt/spark/bin/spark-submit /workspace/scripts/construir_gold_historico_fuente1_v2.py
"""

import hashlib
import json
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

SOURCE_SHA = "e6e88de8b58ab84f25159d63584300e453d9bb7fe5057f5f31976a602f2ca29a"
SILVER = os.getenv(
    "OSIPTEL_SILVER_F1",
    "hdfs://namenode:8020/data/silver/conexiones_fuente1/v1_" + SOURCE_SHA[:20],
)
GOLD_BASE = os.getenv("OSIPTEL_GOLD_BASE", "hdfs://namenode:8020/data/gold")
RULES = "osiptel-historico-f1-v2-dedup7-depto-separado-huellas"
VERSION = hashlib.sha256((SOURCE_SHA + "|" + RULES).encode("utf-8")).hexdigest()[:20]
KEYS = [
    "mes", "empresa", "tecnologia", "segmento", "departamento",
    "rango_velocidad", "conexiones",
]


def anunciar(mensaje):
    print("[GOLD_F1_V2] " + mensaje, flush=True)


def esquema(df):
    return [(f.name, f.dataType.simpleString()) for f in
            sorted(df.schema.fields, key=lambda campo: campo.name)]


def firma(df):
    """Huella independiente del orden y sensible a multiplicidad de filas.

    Cada fila se serializa con todas las columnas (incluidos los nulls), se
    calcula SHA-256 y se acumulan DOS fragmentos de 60 bits como DECIMAL(38,0).
    El conteo, la tipificacion y los controles de sumas se verifican aparte.
    """
    nombres = sorted(df.columns)
    estructura = F.struct(*[F.col(c).alias(c) for c in nombres])
    serial = F.to_json(estructura, options={"ignoreNullFields": "false"})
    digest = F.sha2(serial, 256)
    huellas = df.select(
        F.conv(F.substring(digest, 1, 15), 16, 10).cast("decimal(38,0)").alias("h1"),
        F.conv(F.substring(digest, 17, 15), 16, 10).cast("decimal(38,0)").alias("h2"),
    )
    fila = huellas.agg(
        F.count("*").alias("filas"),
        F.sum("h1").alias("suma_h1"),
        F.sum("h2").alias("suma_h2"),
        F.sum(F.when(F.col("h1").isNull() | F.col("h2").isNull(), 1)
              .otherwise(0)).alias("nulos_firma"),
    ).first()
    if fila.nulos_firma:
        raise RuntimeError("No se pudo calcular una huella de contenido")
    return {
        "filas": int(fila.filas),
        "suma_h1": str(fila.suma_h1),
        "suma_h2": str(fila.suma_h2),
    }


def verificar_publicacion(esperado, observado, firma_esperada, nombre):
    if esquema(esperado) != esquema(observado):
        raise RuntimeError("Esquema diferente en " + nombre)
    firma_observada = firma(observado)
    if firma_observada != firma_esperada:
        raise RuntimeError(
            "Huella distinta en " + nombre + ": esperado=" +
            str(firma_esperada) + ", observado=" + str(firma_observada)
        )
    return firma_observada["filas"]


def publicar(spark, nombre, df):
    base = GOLD_BASE.rstrip("/") + "/" + nombre
    final = base + "/v2_" + VERSION
    temporal = base + "/_staging_v2_" + VERSION
    PathH = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    fs = PathH(base).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())

    anunciar("Calculando huella esperada: " + nombre)
    firma_esperada = firma(df)
    anunciar("HUELLA_ESPERADA " + nombre + " " + json.dumps(firma_esperada))

    if fs.exists(PathH(final)):
        anunciar("Validando snapshot existente: " + final)
        n = verificar_publicacion(df, spark.read.parquet(final), firma_esperada, nombre)
        estado = "EXISTENTE_VERIFICADO"
    else:
        if fs.exists(PathH(temporal)):
            raise RuntimeError("Staging v2 preexistente; inspeccionar manualmente: " + temporal)
        anunciar("Escribiendo staging: " + temporal)
        (df.coalesce(2).write.mode("errorifexists")
         .option("compression", "snappy").parquet(temporal))
        anunciar("Validando Parquet del staging: " + nombre)
        verificar_publicacion(df, spark.read.parquet(temporal), firma_esperada, nombre)
        anunciar("Publicando con rename: " + final)
        if not fs.rename(PathH(temporal), PathH(final)):
            raise RuntimeError("No se pudo renombrar staging a final: " + final)
        anunciar("Verificando snapshot final: " + nombre)
        n = verificar_publicacion(df, spark.read.parquet(final), firma_esperada, nombre)
        estado = "PUBLICADO_Y_VERIFICADO"

    anunciar("GOLD_HISTORICO: " + nombre + " " + estado + " filas=" + str(n))
    return {"tabla": nombre, "estado": estado, "filas": n,
            "ruta_hdfs": final, "firma": firma_esperada}


def limpio_o_etiqueta(columna, etiqueta):
    valor = F.trim(F.coalesce(F.col(columna), F.lit("")))
    return F.when(valor == "", F.lit(etiqueta)).otherwise(valor)


def main():
    spark = (SparkSession.builder
             .appName("OSIPTEL_Gold_Historico_F1_V2")
             .master("local[2]")
             .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    persistidos = []
    try:
        anunciar("Leyendo Silver: " + SILVER)
        fuente = spark.read.parquet(SILVER)
        requeridas = set(KEYS + [
            "periodo", "estado_validacion", "sha256_origen", "fila_excel",
            "empresa_normalizada", "departamento_normalizado",
            "tecnologia_normalizada", "segmento_normalizado",
        ])
        faltantes = sorted(requeridas - set(fuente.columns))
        if faltantes:
            raise RuntimeError("Faltan columnas Silver: " + str(faltantes))

        audit = fuente.agg(
            F.count("*").alias("filas"),
            F.countDistinct("periodo").alias("periodos"),
            F.countDistinct("sha256_origen").alias("shas"),
            F.min("sha256_origen").alias("sha"),
            F.sum(F.when(
                F.col("conexiones").isNull() |
                (F.col("conexiones") < 0) |
                F.col("periodo").isNull() |
                (F.col("estado_validacion") == "RECHAZADO"), 1
            ).otherwise(0)).alias("invalidos"),
        ).first()
        if (audit.filas != 141106 or audit.periodos != 71 or audit.shas != 1 or
                audit.sha != SOURCE_SHA or audit.invalidos != 0):
            raise RuntimeError("Snapshot Silver distinto del auditado: " + str(audit))
        anunciar("SILVER_VALIDADO: filas=141106 periodos=71")

        sin_duplicados = fuente.dropDuplicates(KEYS).persist(StorageLevel.MEMORY_AND_DISK)
        persistidos.append(sin_duplicados)
        total_unicos = sin_duplicados.count()
        if total_unicos != 138620:
            raise RuntimeError("Unicos: " + str(total_unicos) + "; se esperaban 138620")
        depto_vacio = (F.col("departamento").isNull() |
                       (F.trim(F.col("departamento")) == ""))
        territorial = sin_duplicados.filter(~depto_vacio).persist(StorageLevel.MEMORY_AND_DISK)
        persistidos.append(territorial)
        filas_territoriales = territorial.count()
        sin_departamento = total_unicos - filas_territoriales
        if (sin_departamento != 201 or filas_territoriales != 138419 or
                audit.filas != 2486 + sin_departamento + filas_territoriales):
            raise RuntimeError("Conciliacion territorial inconsistente")
        anunciar("CONCILIACION_OK: duplicados=2486 sin_departamento=201 territoriales=138419")

        # Stock por periodo; no sumar periodos distintos para medir conexiones actuales.
        nacional = (sin_duplicados.groupBy("periodo")
                    .agg(F.sum("conexiones").cast("long").alias("conexiones_fijas"),
                         F.count("*").cast("long").alias("filas_unicas"),
                         F.sum(F.when(depto_vacio, 1).otherwise(0)).cast("long")
                          .alias("filas_sin_departamento"))
                    .withColumn("origen_sha256", F.lit(SOURCE_SHA))
                    .withColumn("regla_version", F.lit(RULES))
                    .persist(StorageLevel.MEMORY_AND_DISK))
        persistidos.append(nacional)
        if nacional.count() != 71:
            raise RuntimeError("La serie nacional no tiene 71 periodos")
        cierre = {r["periodo"]: r["conexiones_fijas"] for r in
                  nacional.filter(F.col("periodo").isin("2025-12", "2026-03"))
                  .select("periodo", "conexiones_fijas").collect()}
        if cierre != {"2025-12": 4379544, "2026-03": 4135995}:
            raise RuntimeError("Cierres nacionales incongruentes: " + str(cierre))
        sumas = nacional.agg(
            F.sum("filas_unicas").alias("unicas"),
            F.sum("filas_sin_departamento").alias("sin_depto"),
        ).first()
        if sumas.unicas != 138620 or sumas.sin_depto != 201:
            raise RuntimeError("Sumas de filas nacionales inconsistentes")
        anunciar("NACIONAL_VALIDADO: periodos=71 cierres=2025-12/2026-03")

        hechos = (territorial
                  .withColumn("operador", limpio_o_etiqueta("empresa_normalizada", "NO IDENTIFICADA"))
                  .withColumn("departamento", limpio_o_etiqueta("departamento_normalizado", "SIN DEPARTAMENTO"))
                  .withColumn("tecnologia", limpio_o_etiqueta("tecnologia_normalizada", "NO DECLARADA"))
                  .withColumn("segmento", limpio_o_etiqueta("segmento_normalizado", "NO DECLARADO"))
                  .withColumn("rango_velocidad", limpio_o_etiqueta("rango_velocidad", "NO DECLARADO")))
        agrupadas = ["periodo", "departamento", "operador", "tecnologia", "segmento", "rango_velocidad"]
        historia = (hechos.groupBy(*agrupadas)
                   .agg(F.sum("conexiones").cast("long").alias("conexiones_fijas"),
                        F.count("*").cast("long").alias("filas_origen"),
                        F.sum(F.when(F.col("estado_validacion") == "OBSERVADO", 1)
                              .otherwise(0)).cast("long").alias("filas_observadas"))
                   .withColumn("origen_sha256", F.lit(SOURCE_SHA))
                   .withColumn("regla_version", F.lit(RULES))
                   .persist(StorageLevel.MEMORY_AND_DISK))
        persistidos.append(historia)
        historico_stats = historia.agg(
            F.count("*").alias("filas_gold"),
            F.sum("filas_origen").alias("filas_origen"),
            F.sum(F.when(F.col("conexiones_fijas").isNull() |
                         (F.col("conexiones_fijas") < 0), 1).otherwise(0))
            .alias("invalidos"),
        ).first()
        if historico_stats.filas_origen != 138419 or historico_stats.invalidos != 0:
            raise RuntimeError("Gold territorial no reconcilia con Silver")
        anunciar("TERRITORIAL_VALIDADO: filas_gold=" + str(historico_stats.filas_gold) +
                 " filas_origen=138419")

        publicaciones = [
            publicar(spark, "osiptel_historico_nacional_f1", nacional),
            publicar(spark, "osiptel_historico_territorial_f1", historia),
        ]
        resumen = {
            "snapshot_silver": SILVER,
            "filas_originales": 141106,
            "duplicados_exactos_excluidos": 2486,
            "filas_sin_departamento_reservadas_para_nacional": 201,
            "filas_territoriales": 138419,
            "periodos": 71,
            "cierres": cierre,
            "nota": "Conexiones son stocks por mes; no sumar varios meses como usuarios actuales.",
            "publicaciones": publicaciones,
        }
        print("=== RESUMEN GOLD HISTORICO F1 V2 ===", flush=True)
        print(json.dumps(resumen, indent=2, ensure_ascii=False), flush=True)
    finally:
        for df in reversed(persistidos):
            try:
                df.unpersist(blocking=False)
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    main()
