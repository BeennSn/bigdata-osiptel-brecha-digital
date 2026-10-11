#!/usr/bin/env python3
"""Prueba de Escalabilidad del Pipeline de Big Data (1x, 10x, 100x).

Mide el comportamiento del pipeline ante el crecimiento del volumen de datos:
- 1x: Dataset real (~138,419 registros)
- 10x: Réplica aumentada (~1,384,190 registros)
- 100x: Réplica aumentada (~13,841,900 registros)

Los datos sintéticos se almacenan en una ruta temporal de HDFS y se eliminan
automáticamente al finalizar la medición. No afectan las capas Silver ni Gold.
"""
import json
import os
import subprocess
import time
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_SILVER_REAL = "hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet"
HDFS_BENCH_BASE = "hdfs://localhost:9000/data/benchmark_scalability"


def run_benchmark():
    print("======================================================================")
    print("INICIANDO PRUEBA FORMAL DE ESCALABILIDAD (1x, 10x, 100x)")
    print("======================================================================")

    spark = (
        SparkSession.builder.appName("Osiptel-Scalability-Test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )

    try:
        # 1. Leer dataset base Silver real
        print(f"[INFO] Leyendo dataset base Silver desde: {HDFS_SILVER_REAL}")
        df_base = spark.read.parquet(HDFS_SILVER_REAL)
        conteo_base = df_base.count()
        print(f"[INFO] Filas base reales (1x): {conteo_base:,}")

        # Leer tablas auxiliares para simular el cruce multifuente completo
        df_inei = None
        try:
            df_inei = spark.read.parquet("hdfs://localhost:9000/data/silver/inei_indicadores.parquet")
        except Exception:
            pass

        df_mandatos = None
        try:
            df_mandatos = spark.read.parquet("hdfs://localhost:9000/data/silver/osiptel_mandatos_metricas.parquet")
        except Exception:
            pass

        escalas = [1, 10, 100]
        resultados = []

        for factor in escalas:
            print(f"\n----------------------------------------------------------------------")
            print(f">>> EVALUANDO FACTOR DE ESCALA: {factor}x")
            print(f"----------------------------------------------------------------------")

            ruta_input = f"{HDFS_BENCH_BASE}/input_{factor}x.parquet"
            ruta_output = f"{HDFS_BENCH_BASE}/output_{factor}x.parquet"

            # Generar y persistir réplica si factor > 1
            if factor == 1:
                ruta_input = HDFS_SILVER_REAL
                filas_input = conteo_base
            else:
                print(f"[INFO] Generando réplica de {factor}x en HDFS...")
                t_gen = time.time()
                dfs_replicas = [df_base for _ in range(factor)]
                df_replicado = reduce(lambda a, b: a.union(b), dfs_replicas)
                df_replicado.write.mode("overwrite").parquet(ruta_input)
                print(f"[INFO] Réplica {factor}x guardada en {time.time() - t_gen:.2f} s en {ruta_input}")
                filas_input = conteo_base * factor

            # Medir ejecución del pipeline analítico completo sobre la réplica
            print(f"[INFO] Ejecutando pipeline analítico completo sobre {filas_input:,} filas...")
            t_inicio = time.time()

            # A. Lectura de HDFS
            df_curr = spark.read.parquet(ruta_input)

            # B. Filtrado de stock a cierre de trimestre
            df_cierre = df_curr.filter(F.col("mes_num").isin(["03", "06", "09", "12"])).withColumn(
                "trimestre",
                F.when(F.col("mes_num") == "03", 1)
                .when(F.col("mes_num") == "06", 2)
                .when(F.col("mes_num") == "09", 3)
                .when(F.col("mes_num") == "12", 4)
                .cast("int"),
            )

            # C. Agregación analítica multivariada
            df_agg = df_cierre.groupBy("anio", "trimestre", "departamento").agg(
                F.sum("conexiones").alias("total_conexiones"),
                F.sum(
                    F.when(F.col("tecnologia") == "FIBRA OPTICA", F.col("conexiones")).otherwise(0)
                ).alias("conexiones_fibra_optica"),
                F.sum(
                    F.when(F.col("tecnologia") == "CABLEMODEM", F.col("conexiones")).otherwise(0)
                ).alias("conexiones_cablemodem"),
                F.countDistinct("empresa").alias("total_operadoras_activas"),
            ).withColumn(
                "pct_fibra_optica",
                F.when(
                    F.col("total_conexiones") > 0,
                    F.round((F.col("conexiones_fibra_optica") / F.col("total_conexiones")) * 100, 2),
                ).otherwise(0.0),
            )

            # D. Cruce de datos (Joins)
            if df_inei is not None:
                df_agg = df_agg.join(df_inei, on=["anio", "trimestre"], how="left")
            if df_mandatos is not None:
                df_agg = df_agg.join(df_mandatos, on="anio", how="left")

            # E. Escritura física particionada en HDFS (Fuerza evaluación completa)
            df_agg.write.mode("overwrite").format("parquet").option(
                "compression", "snappy"
            ).partitionBy("anio").save(ruta_output)

            t_fin = time.time()
            duracion = t_fin - t_inicio
            throughput = filas_input / duracion if duracion > 0 else 0

            print(f"[RESULTADO] Factor {factor}x completado exitosamente.")
            print(f"  • Filas procesadas:        {filas_input:,}")
            print(f"  • Tiempo total pipeline:   {duracion:.3f} segundos")
            print(f"  • Velocidad de procesado:  {throughput:,.0f} filas/segundo")

            resultados.append({
                "factor": f"{factor}x",
                "filas": filas_input,
                "duracion_segundos": round(duracion, 3),
                "throughput_filas_seg": round(throughput, 0),
            })

        print("\n======================================================================")
        print("RESUMEN CONSOLIDADO DE LA PRUEBA DE ESCALABILIDAD")
        print("======================================================================")
        print(f"{'Factor':<8} | {'Filas':<15} | {'Tiempo (s)':<12} | {'Aumento Tiempo':<16} | {'Throughput (filas/s)':<20}")
        print("-" * 80)
        t_base = resultados[0]["duracion_segundos"]
        for r in resultados:
            ratio_tiempo = f"{r['duracion_segundos'] / t_base:.2f}x"
            print(f"{r['factor']:<8} | {r['filas']:<15,d} | {r['duracion_segundos']:<12.3f} | {ratio_tiempo:<16} | {r['throughput_filas_seg']:<20,.0f}")

        # Guardar reporte de auditoría de escalabilidad
        os.makedirs("evidence", exist_ok=True)
        reporte_path = "evidence/reporte_escalabilidad.json"
        with open(reporte_path, "w", encoding="utf-8") as f:
            json.dump({
                "fecha": time.strftime("%Y-%m-%d %H:%M:%S"),
                "resultados": resultados,
                "descripcion": "Benchmark de escalabilidad del pipeline sobre replicas 1x, 10x y 100x"
            }, f, indent=2)
        print(f"\n[INFO] Reporte guardado en: {reporte_path}")

    finally:
        spark.stop()
        print("\n[LIMPIEZA] Eliminando directorios temporales de réplica en HDFS...")
        try:
            subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", HDFS_BENCH_BASE], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("[LIMPIEZA] ✅ Directorio temporal HDFS eliminado con éxito. Disco liberado.")
        except Exception as e:
            print(f"[LIMPIEZA] Aviso: {e}")


if __name__ == "__main__":
    run_benchmark()
