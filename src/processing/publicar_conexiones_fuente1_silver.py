"""Publicacion versionada de Fuente 1 (OSIPTEL) en Silver/Parquet por Spark.

Entrada: CSV preparado y reporte JSON en /workspace/scripts/inputs/.
Salida: snapshot inmutable en HDFS, versionado por SHA256 del XLSX.
No modifica Bronze ni las rutas Silver antiguas.
"""

import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

INPUT_CSV = "file:/workspace/scripts/inputs/conexiones_fuente1.csv"
REPORT = Path("/workspace/scripts/inputs/preparacion_conexiones_silver.json")
HDFS_BASE = "hdfs://namenode:8020/data/silver/conexiones_fuente1"

COLUMNS = [
    "fila_excel", "mes_excel", "mes", "empresa", "tecnologia", "segmento",
    "departamento", "rango_velocidad", "conexiones_original", "conexiones",
    "estado_validacion", "observaciones", "archivo_origen", "sha256_origen",
]


def verificar(df, meta):
    """Impide publicar si se pierden filas, totales, periodos o trazabilidad."""
    resumen = df.agg(
        F.count("*").alias("filas"),
        F.sum("conexiones").alias("conexiones"),
        F.sum(F.when(F.col("estado_validacion") == "RECHAZADO", 1).otherwise(0)).alias("rechazados"),
        F.countDistinct("fila_excel").alias("filas_excel_distintas"),
        F.countDistinct("periodo").alias("periodos"),
        F.sum(F.when(F.col("mes").isNull() | F.col("conexiones").isNull(), 1).otherwise(0)).alias("nulos_criticos"),
        F.countDistinct("sha256_origen").alias("sha_distintos"),
        F.min("sha256_origen").alias("sha_min"),
    ).first()

    esperado_filas = int(meta["filas_procesadas"])
    esperado_conexiones = int(meta["suma_conexiones_filas_validas"])
    esperado_periodos = int(meta["periodos"])
    esperado_rechazados = int(meta["filas_rechazadas"])

    controles = {
        "filas": (int(resumen.filas), esperado_filas),
        "conexiones": (int(resumen.conexiones or 0), esperado_conexiones),
        "periodos": (int(resumen.periodos), esperado_periodos),
        "rechazados": (int(resumen.rechazados or 0), esperado_rechazados),
        "filas_excel_distintas": (int(resumen.filas_excel_distintas), esperado_filas),
        "nulos_criticos": (int(resumen.nulos_criticos or 0), 0),
        "sha_distintos": (int(resumen.sha_distintos), 1),
        "sha_min": (resumen.sha_min, meta["sha256_origen"]),
    }
    for nombre, (real, esperado) in controles.items():
        print(f"CONTROL {nombre}: obtenido={real}, esperado={esperado}")
        if real != esperado:
            raise RuntimeError(f"CONTROL FALLIDO: {nombre}: {real} != {esperado}")

    return {
        "filas": int(resumen.filas),
        "conexiones_suma_historica": int(resumen.conexiones or 0),
        "periodos": int(resumen.periodos),
        "rechazados": int(resumen.rechazados or 0),
    }


def main():
    meta = json.loads(REPORT.read_text(encoding="utf-8"))
    sha = meta["sha256_origen"]
    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha.lower()):
        raise ValueError("SHA256 de origen invalido")

    spark = (
        SparkSession.builder.appName("OSIPTEL_Fuente1_Silver_V1")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        schema = StructType([StructField(c, StringType(), True) for c in COLUMNS])
        origen = (
            spark.read.option("header", "true")
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(INPUT_CSV)
        )
        datos = (
            origen
            .withColumn("fila_excel", F.col("fila_excel").cast("long"))
            .withColumn("mes", F.to_date("mes", "yyyy-MM-dd"))
            .withColumn("conexiones", F.col("conexiones").cast("long"))
            .withColumn("periodo", F.date_format("mes", "yyyy-MM"))
            .withColumn("empresa_normalizada", F.upper(F.trim(F.regexp_replace("empresa", r"\s+", " "))))
            .withColumn("departamento_normalizado", F.upper(F.trim("departamento")))
            .withColumn("segmento_normalizado", F.upper(F.trim("segmento")))
            .withColumn("tecnologia_normalizada", F.upper(F.trim("tecnologia")))
            .withColumn("ingesta_sha256", F.lit(sha))
        )
        resumen = verificar(datos, meta)

        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        ruta_final = f"{HDFS_BASE}/v1_{sha[:20]}"
        ruta_temporal = f"{HDFS_BASE}/_staging_v1_{sha[:20]}"
        fs = jvm.org.apache.hadoop.fs.Path(HDFS_BASE).getFileSystem(conf)
        path_final = jvm.org.apache.hadoop.fs.Path(ruta_final)
        path_temporal = jvm.org.apache.hadoop.fs.Path(ruta_temporal)

        if fs.exists(path_final):
            print("Snapshot ya existente; validando antes de omitir escritura.")
            verificar(spark.read.parquet(ruta_final), meta)
            estado = "EXISTENTE_VERIFICADO"
        else:
            if fs.exists(path_temporal):
                raise RuntimeError(
                    "Existe un staging previo; requiere inspeccion manual antes de reintentar: "
                    + ruta_temporal
                )
            print("Escribiendo snapshot temporal:", ruta_temporal)
            datos.coalesce(2).write.mode("errorifexists").parquet(ruta_temporal)
            verificar(spark.read.parquet(ruta_temporal), meta)
            if not fs.rename(path_temporal, path_final):
                raise RuntimeError("No se pudo publicar mediante rename atomico en HDFS")
            estado = "PUBLICADO_Y_VERIFICADO"

        print("\n=== SILVER FUENTE 1 ===")
        print(json.dumps({
            "estado": estado,
            "ruta_hdfs": ruta_final,
            "sha256_bronze": sha,
            **resumen,
            "nota": "La suma de conexiones abarca todos los periodos; no es un total de usuarios actuales.",
        }, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
