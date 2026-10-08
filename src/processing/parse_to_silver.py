import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, length
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

MANIFEST_PATH = "data/manifest.json"

def procesar_a_silver_con_yarn():
    print("=== INICIANDO PROCESAMIENTO RAW -> SILVER (SPARK + YARN + HDFS) ===")
    
    # 1. Validar existencia del contrato de datos (manifest.json)
    if not os.path.exists(MANIFEST_PATH):
        print(f"[ERROR] No se encontró el manifiesto en: {MANIFEST_PATH}. Ejecuta la ingesta primero.")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)
    
    archivos_permitidos = manifest_data.get("archivos", [])
    print(f"[INFO] Contrato cargado. Archivos registrados en manifiesto: {len(archivos_permitidos)}")

    if not archivos_permitidos:
        print("[ADVERTENCIA] El manifiesto no contiene archivos para procesar.")
        return

    # 2. Inicializar la sesión de Spark apuntando a YARN y HDFS
    spark = SparkSession.builder \
        .appName("Osiptel-RawToSilver-YarnPipeline") \
        .config("spark.master", "yarn") \
        .getOrCreate()

    # Rutas HDFS de la arquitectura
    hdfs_raw_path = "hdfs://localhost:9000/data/raw/osiptel_pdfs/*"
    hdfs_silver_path = "hdfs://localhost:9000/data/silver/osiptel_textos_limpios.parquet"

    print(f"[INFO] Conectando a HDFS y YARN para leer la capa Raw: {hdfs_raw_path}")

    try:
        # 3. Lectura de archivos binarios desde HDFS mediante Spark
        df_raw = spark.read.format("binaryFile").load(hdfs_raw_path)

        # Filtrar estrictamente usando el contrato del manifest.json
        # Extraemos el nombre del archivo de la ruta completa de HDFS para cruzarlo
        from pyspark.sql.functions import element_at, split
        df_filtrado = df_raw.withColumn("nombre_archivo", element_at(split(col("path"), "/"), -1)) \
                            .filter(col("nombre_archivo").isin(archivos_permitidos))

        total_procesar = df_filtrado.count()
        print(f"[INFO] Archivos validados por el manifiesto listos para procesar en YARN: {total_procesar}")

        if total_procesar == 0:
            print("[ADVERTENCIA] Ninguno de los archivos en HDFS coincide con el manifiesto.")
            return

        # 4. Definición del Esquema Estrictamente Tipado para la Capa Silver
        # Aqui estructuramos la información columnar que exige tu arquitectura
        esquema_silver = StructType([
            StructField("doc_id", StringType(), False),
            StructField("nombre_archivo", StringType(), False),
            StructField("ruta_hdfs", StringType(), False),
            StructField("tamanio_bytes", LongType(), True),
            StructField("capa_origen", StringType(), False),
            StructField("estado_procesamiento", StringType(), False)
        ])

        # 5. Transformaciones distribuidas ejecutadas por YARN
        df_silver = df_filtrado.select(
            col("path").alias("ruta_hdfs"),
            col("length").alias("tamanio_bytes"),
            element_at(split(col("path"), "/"), -1).alias("nombre_archivo")
        ).withColumn("doc_id", col("nombre_archivo")) \
         .withColumn("capa_origen", lit("raw")) \
         .withColumn("estado_procesamiento", lit("silver_estructurado"))

        print(f"[INFO] Escribiendo resultados con esquema definido en la Capa Silver (HDFS): {hdfs_silver_path}")
        
        # 6. Escritura en HDFS en formato Parquet (Capa Silver)
        df_silver.write \
            .mode("overwrite") \
            .parquet(hdfs_silver_path)

        print("\n[ÉXITO] Procesamiento completado en YARN. Capa Silver guardada en Parquet.")
        df_silver.show(5, truncate=False)

    except Exception as e:
        print(f"[ERROR] Falló la ejecución del pipeline en YARN: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    procesar_a_silver_con_yarn()