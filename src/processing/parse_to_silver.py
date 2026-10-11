import os
import json
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, element_at, split

MANIFEST_PATH = "data/manifest.json"

# Ajusta esta ruta a la carpeta real de la capa Raw en HDFS (comprueba con: hdfs dfs -ls /data/raw/)
HDFS_RAW_DIR = "hdfs://localhost:9000/data/raw/osiptel_pdfs/"
HDFS_SILVER_PATH = "hdfs://localhost:9000/data/silver/osiptel_textos_limpios.parquet"


def _hdfs_path_existe(ruta_hdfs: str) -> bool:
    """Verifica si una ruta existe en HDFS mediante hdfs dfs -test -e."""
    try:
        res = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", ruta_hdfs],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return res.returncode == 0
    except Exception:
        return False


def procesar_a_silver_con_yarn(forzar: bool = False):
    print("=== INICIANDO PROCESAMIENTO RAW -> SILVER (SPARK + YARN + HDFS) ===")

    # 1. Validar persistencia previa
    if not forzar and _hdfs_path_existe(HDFS_SILVER_PATH):
        print(f"[INFO] La capa Silver de PDFs ya existe en HDFS ({HDFS_SILVER_PATH}).")
        print("[INFO] Se omite el procesamiento por persistencia. Usa forzar=True si requieres reprocesar.")
        return

    # 2. Validar existencia del manifiesto
    if not os.path.exists(MANIFEST_PATH):
        raise FileNotFoundError(
            f"No se encontró el manifiesto en: {MANIFEST_PATH}. Ejecuta la ingesta primero."
        )

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)

    archivos_permitidos = (
        manifest_data.get("archivos")
        or manifest_data.get("datasets", {}).get("osiptel_mandatos", {}).get("archivos", [])
    )
    print(f"[INFO] Manifiesto cargado. PDFs de mandatos registrados: {len(archivos_permitidos)}")

    if not archivos_permitidos:
        print("[ADVERTENCIA] El manifiesto no contiene archivos para procesar.")
        return

    # Ruta de lectura dinámica desde manifiesto
    ruta_manifest = (
        manifest_data.get("ruta_hdfs")
        or manifest_data.get("datasets", {}).get("osiptel_mandatos", {}).get("ruta_hdfs")
        or HDFS_RAW_DIR
    )
    hdfs_dir_lectura = ruta_manifest if ruta_manifest.startswith("hdfs://") else f"hdfs://localhost:9000{ruta_manifest}"

    # 3. Inicializar la sesión de Spark apuntando a YARN
    spark = SparkSession.builder \
        .appName("Osiptel-RawToSilver-YarnPipeline") \
        .config("spark.master", os.getenv("SPARK_MASTER", "yarn")) \
        .getOrCreate()

    print(f"[INFO] Leyendo la capa Raw desde HDFS: {hdfs_dir_lectura}")

    try:
        # 4. Lectura de PDFs como binarios (se ignoran otros archivos, como el XLSX)
        df_raw = spark.read.format("binaryFile") \
            .option("pathGlobFilter", "*.pdf") \
            .load(hdfs_dir_lectura)

        # Filtrar por la lista de archivos del manifiesto
        df_filtrado = df_raw.withColumn("nombre_archivo", element_at(split(col("path"), "/"), -1)) \
                            .filter(col("nombre_archivo").isin(archivos_permitidos))

        total_procesar = df_filtrado.count()
        print(f"[INFO] Archivos incluidos en el manifiesto listos para procesar: {total_procesar}")

        if total_procesar == 0:
            print("[ADVERTENCIA] Ninguno de los archivos en HDFS coincide con el manifiesto.")
            return

        # 5. Metadatos de control del documento
        df_silver = df_filtrado.select(
            col("path").alias("ruta_hdfs"),
            col("length").alias("tamanio_bytes"),
            element_at(split(col("path"), "/"), -1).alias("nombre_archivo")
        ).withColumn("doc_id", col("nombre_archivo")) \
         .withColumn("capa_origen", lit("raw")) \
         .withColumn("estado_procesamiento", lit("silver_estructurado"))

        print(f"[INFO] Escribiendo Silver en HDFS: {HDFS_SILVER_PATH}")

        # 6. Escritura en HDFS en formato Parquet
        df_silver.write \
            .mode("overwrite") \
            .parquet(HDFS_SILVER_PATH)

        print("\n[ÉXITO] Procesamiento completado. Silver de PDFs guardado en Parquet.")
        df_silver.show(5, truncate=False)

    except Exception as e:
        print(f"[ERROR] Falló la ejecución del pipeline en YARN: {e}")
        raise
    finally:
        spark.stop()


def process_general_silver(forzar: bool = False):
    """Punto de entrada estándar para procesamiento de documentos PDF a Silver."""
    return procesar_a_silver_con_yarn(forzar=forzar)


if __name__ == "__main__":
    process_general_silver()