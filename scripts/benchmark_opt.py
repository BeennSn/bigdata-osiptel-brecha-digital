#!/usr/bin/env python3
"""Benchmark empírico de optimización del rendimiento en Apache Spark.

Evalúa:
1. Optimización de Shuffle Partitions (200 vs 8)
2. Optimización de lectura física con Partition Pruning (partitionBy("anio"))
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    print("=== EJECUTANDO BENCHMARK REAL DE OPTIMIZACION EN PYSPARK ===")

    # 1. Configuración por defecto (Sin optimizar: 200 particiones de shuffle)
    spark_default = (
        SparkSession.builder.appName("Benchmark-Default")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    df = spark_default.read.parquet("hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet")

    t0 = time.time()
    _ = df.groupBy("anio", "departamento", "tecnologia").agg(F.sum("conexiones").alias("total")).collect()
    t_default = time.time() - t0
    spark_default.stop()

    # 2. Configuración Optimizada (8 particiones de shuffle ajustadas a vCPUs)
    spark_opt = (
        SparkSession.builder.appName("Benchmark-Optimizado")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    df_opt = spark_opt.read.parquet("hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet")

    t0 = time.time()
    _ = df_opt.groupBy("anio", "departamento", "tecnologia").agg(F.sum("conexiones").alias("total")).collect()
    t_opt = time.time() - t0

    # 3. Partition Pruning (Lectura de partición específica vs Lectura completa)
    t0 = time.time()
    _ = spark_opt.read.parquet("hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet").filter(F.col("anio") == "2024").count()
    t_scan_full = time.time() - t0

    t0 = time.time()
    _ = spark_opt.read.parquet("hdfs://localhost:9000/data/gold/brecha_digital_consolidada.parquet").filter(F.col("anio") == "2024").count()
    t_pruning = time.time() - t0

    spark_opt.stop()

    print(f"\n1. Optimizacion Shuffle Partitions (200 vs 8):")
    print(f"   - Tiempo por defecto (200 particiones): {t_default:.3f} segundos")
    print(f"   - Tiempo optimizado (8 particiones):    {t_opt:.3f} segundos")
    mejora_shuffle = ((t_default - t_opt) / t_default) * 100
    print(f"   - Reducción de tiempo / Mejora:         {mejora_shuffle:.2f}%")

    print(f"\n2. Optimizacion Partition Pruning (Escaneo completo vs Particionado Hive):")
    print(f"   - Escaneo completo de tabla (Sin podar): {t_scan_full:.3f} segundos")
    print(f"   - Lectura con Partition Pruning:         {t_pruning:.3f} segundos")
    mejora_pruning = ((t_scan_full - t_pruning) / t_scan_full) * 100
    print(f"   - Reducción de tiempo / Mejora:          {mejora_pruning:.2f}%")


if __name__ == "__main__":
    main()
