import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, element_at, split

MANIFEST_PATH = "data/manifest.json"

# Ajusta esta ruta a la carpeta real de la capa Raw en HDFS (comprueba con: hdfs dfs -ls /data/raw/)
HDFS_RAW_DIR = "hdfs://localhost:9000/data/raw/osiptel_pdfs/"
HDFS_SILVER_PATH = "hdfs://localhost:9000/data/silver/osiptel_textos_limpios.parquet"


def procesar_a_silver_con_yarn():
    print("=== INICIANDO PROCESAMIENTO RAW -> SILVER (SPARK + YARN + HDFS) ===")

    # 1. Validar existencia del manifiesto
    if not os.path.exists(MANIFEST_PATH):
        print(f"[ERROR] No se encontró el manifiesto en: {MANIFEST_PATH}. Ejecuta la ingesta primero.")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)

    archivos_permitidos = manifest_data.get("archivos", [])
    print(f"[INFO] Manifiesto cargado. PDFs registrados: {len(archivos_permitidos)}")

    if not archivos_permitidos:
        print("[ADVERTENCIA] El manifiesto no contiene archivos para procesar.")
        return

    # 2. Inicializar la sesión de Spark apuntando a YARN
    spark = SparkSession.builder \
        .appName("Osiptel-RawToSilver-YarnPipeline") \
        .config("spark.master", "yarn") \
        .getOrCreate()

    print(f"[INFO] Leyendo la capa Raw desde HDFS: {HDFS_RAW_DIR}")

    try:
        # 3. Lectura de PDFs como binarios (se ignoran otros archivos, como el XLSX)
        df_raw = spark.read.format("binaryFile") \
            .option("pathGlobFilter", "*.pdf") \
            .load(HDFS_RAW_DIR)

        # Filtrar por la lista de archivos del manifiesto
        df_filtrado = df_raw.withColumn("nombre_archivo", element_at(split(col("path"), "/"), -1)) \
                            .filter(col("nombre_archivo").isin(archivos_permitidos))

        total_procesar = df_filtrado.count()
        print(f"[INFO] Archivos incluidos en el manifiesto listos para procesar: {total_procesar}")

        if total_procesar == 0:
            print("[ADVERTENCIA] Ninguno de los archivos en HDFS coincide con el manifiesto.")
            return

        # 4. Metadatos de control del documento
        df_silver = df_filtrado.select(
            col("path").alias("ruta_hdfs"),
            col("length").alias("tamanio_bytes"),
            element_at(split(col("path"), "/"), -1).alias("nombre_archivo")
        ).withColumn("doc_id", col("nombre_archivo")) \
         .withColumn("capa_origen", lit("raw")) \
         .withColumn("estado_procesamiento", lit("silver_estructurado"))

        print(f"[INFO] Escribiendo Silver en HDFS: {HDFS_SILVER_PATH}")

        # 5. Escritura en HDFS en formato Parquet
        df_silver.write \
            .mode("overwrite") \
            .parquet(HDFS_SILVER_PATH)

        print("\n[ÉXITO] Procesamiento completado. Silver guardado en Parquet.")
        df_silver.show(5, truncate=False)

    except Exception as e:
        print(f"[ERROR] Falló la ejecución del pipeline en YARN: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_a_silver_con_yarn()