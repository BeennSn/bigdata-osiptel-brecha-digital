import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_SILVER_CONEXIONES = "hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet"
HDFS_GOLD_PATH = "hdfs://localhost:9000/data/gold/brecha_digital_consolidada.parquet"
REPORT_GOLD_PATH = Path("evidence/reporte_gold.json")

# Rutas potenciales de indicadores INEI si fueron generados previamente
HDFS_INEI_CANDIDATAS = [
    "hdfs://localhost:9000/data/silver/inei_indicadores.parquet",
    "hdfs://localhost:9000/datalake/osiptel_proyecto/silver/inei_indicadores_versiones",
]


def _hdfs_path_existe(ruta_hdfs: str) -> bool:
    """Verifica si una ruta existe en HDFS mediante hdfs dfs -test -e."""
    try:
        res = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", ruta_hdfs],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return res.returncode == 0
    except Exception:
        return False


def procesar_a_gold_con_yarn(forzar: bool = False):
    print("=== INICIANDO PROCESAMIENTO SILVER -> GOLD (INTEGRACIÓN ANALÍTICA) ===")

    # 1. Verificación de persistencia previa
    if not forzar and _hdfs_path_existe(HDFS_GOLD_PATH) and REPORT_GOLD_PATH.exists():
        print(f"[INFO] La capa Gold consolidada ya existe en HDFS ({HDFS_GOLD_PATH}).")
        print(f"[INFO] Reporte existente en {REPORT_GOLD_PATH}.")
        print("[INFO] Se omite el procesamiento por persistencia. Usa forzar=True si requieres reprocesar.")
        with open(REPORT_GOLD_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    # 2. Validar que la capa Silver de conexiones exista
    if not _hdfs_path_existe(HDFS_SILVER_CONEXIONES):
        raise FileNotFoundError(
            f"No se encontró la capa Silver en {HDFS_SILVER_CONEXIONES}. "
            "Ejecuta primero el procesamiento Silver de conexiones."
        )

    # 3. Inicializar SparkSession apuntando al clúster YARN
    spark = (
        SparkSession.builder.appName("Osiptel-Inei-Gold-Integracion")
        .master(os.getenv("SPARK_MASTER", "yarn"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        print(f"[INFO] Leyendo capa Silver de conexiones desde: {HDFS_SILVER_CONEXIONES}")
        df_silver = spark.read.parquet(HDFS_SILVER_CONEXIONES)

        # 4. Agregación a Cierre de Trimestre (Marzo=1, Junio=2, Septiembre=3, Diciembre=4)
        # Nota técnica: 'Conexiones' es un stock (foto acumulada al cierre de mes).
        # Tomar el mes de cierre de trimestre evita triplicar usuarios en el trimestre.
        df_cierre = df_silver.filter(F.col("mes_num").isin(["03", "06", "09", "12"])).withColumn(
            "trimestre",
            F.when(F.col("mes_num") == "03", 1)
            .when(F.col("mes_num") == "06", 2)
            .when(F.col("mes_num") == "09", 3)
            .when(F.col("mes_num") == "12", 4)
            .cast("int"),
        )

        print("[INFO] Agrupando conexiones fijas a nivel trimestral y departamental...")
        df_osiptel_trim = (
            df_cierre.groupBy("anio", "trimestre", "departamento")
            .agg(
                F.sum("conexiones").cast("long").alias("total_conexiones"),
                F.sum(
                    F.when(F.col("tecnologia") == "FIBRA OPTICA", F.col("conexiones")).otherwise(0)
                )
                .cast("long")
                .alias("conexiones_fibra_optica"),
                F.sum(
                    F.when(F.col("tecnologia") == "CABLEMODEM", F.col("conexiones")).otherwise(0)
                )
                .cast("long")
                .alias("conexiones_cablemodem"),
                F.countDistinct("empresa").cast("int").alias("total_operadoras_activas"),
                F.max("mes").alias("fecha_corte_trimestre"),
                F.sum(F.when(F.col("es_atipico"), 1).otherwise(0)).cast("int").alias("filas_atipicas_detectadas"),
            )
            .withColumn(
                "pct_fibra_optica",
                F.when(
                    F.col("total_conexiones") > 0,
                    F.round(
                        (F.col("conexiones_fibra_optica") / F.col("total_conexiones")) * 100, 2
                    ),
                ).otherwise(0.0),
            )
        )

        # 5. Detección y unión con indicadores del INEI si existen en HDFS
        df_inei = None
        ruta_inei_usada = None
        for candidata in HDFS_INEI_CANDIDATAS:
            if _hdfs_path_existe(candidata):
                try:
                    df_inei = spark.read.parquet(candidata)
                    ruta_inei_usada = candidata
                    print(f"[INFO] Indicadores INEI encontrados en HDFS: {candidata}")
                    break
                except Exception:
                    pass

        # 6. Integración mediante LEFT JOIN (preservando todas las conexiones de OSIPTEL)
        if df_inei is not None and "trimestre" in df_inei.columns and "anio" in df_inei.columns:
            print("[INFO] Aplicando LEFT JOIN con serie histórica del INEI...")
            # Clave de join: si INEI tiene departamento, usar clave compuesta completa
            claves_join = ["anio", "trimestre"]
            if "departamento" in df_inei.columns:
                claves_join.append("departamento")

            df_gold = (
                df_osiptel_trim.join(df_inei, on=claves_join, how="left")
                .withColumn(
                    "estado_integracion",
                    F.when(F.col("hogares_internet_pct").isNull(), F.lit("SIN_DATO_INEI"))
                    .otherwise(F.lit("CONSOLIDADO")),
                )
            )
        else:
            print(
                "[INFO] Serie INEI no detectada en HDFS; generando estructura analítica Gold "
                "con columnas preparadas para cruce multivariado."
            )
            df_gold = (
                df_osiptel_trim.withColumn("hogares_internet_pct", F.lit(None).cast("double"))
                .withColumn("computadora_pct", F.lit(None).cast("double"))
                .withColumn("estado_integracion", F.lit("OSIPTEL_CONSOLIDADO_PENDIENTE_INEI"))
            )

        # 7. Escritura en HDFS en formato Parquet comprimido con Snappy particionado por anio
        print(f"[INFO] Escribiendo dataset analítico Gold en: {HDFS_GOLD_PATH}")
        df_gold.write.mode("overwrite").format("parquet").option(
            "compression", "snappy"
        ).partitionBy("anio").save(HDFS_GOLD_PATH)

        # 8. Métricas de resumen de la capa Gold
        total_filas_gold = df_gold.count()
        departamentos_cubiertos = df_gold.select("departamento").distinct().count()
        anios_cubiertos = df_gold.select("anio").distinct().count()

        reporte_gold = {
            "fecha_generacion": datetime.now().isoformat(),
            "capa_origen": HDFS_SILVER_CONEXIONES,
            "capa_destino_gold": HDFS_GOLD_PATH,
            "total_registros_analiticos": int(total_filas_gold),
            "departamentos_cubiertos": int(departamentos_cubiertos),
            "anios_cubiertos": int(anios_cubiertos),
            "metrica_temporal": "Cierre de Trimestre (Marzo, Junio, Septiembre, Diciembre)",
            "fuente_inei_incorporada": ruta_inei_usada or "PENDIENTE_DE_CARGA",
            "particionamiento": "anio (Snappy Parquet)",
        }

        REPORT_GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(REPORT_GOLD_PATH, "w", encoding="utf-8") as f:
            json.dump(reporte_gold, f, indent=2, ensure_ascii=False)

        print(f"\n[ÉXITO] Capa Gold generada exitosamente. Total registros: {total_filas_gold}")
        print(f"[ÉXITO] Reporte guardado en: {REPORT_GOLD_PATH}")
        print(json.dumps(reporte_gold, indent=2, ensure_ascii=False))
        return reporte_gold

    except Exception as exc:
        print(f"[ERROR] Falló la generación de la capa Gold: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_a_gold_con_yarn()
