#!/usr/bin/env python3
"""Demostracion reproducible de lotes cronologicos, sin tocar el pipeline oficial.

Ejecutar dentro de osiptel-bigdata-spark-1 con spark-submit --master local[2]
  --fase base | delta | replay
Bronze DEMO: CSV derivado del CSV previamente preparado; NO es el XLSX crudo.
"""
import argparse
import json
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType

parser = argparse.ArgumentParser(description="Prueba incremental OSIPTEL aislada")
parser.add_argument("--fase", choices=("base", "delta", "replay"), required=True)
args = parser.parse_args()

T0 = time.monotonic()
ROOT = "hdfs://namenode:8020/data/demo_incremental_osiptel_v1"
ORIGEN = "/workspace/scripts/inputs/conexiones_fuente1.csv"
BASE_N = 136575
DELTA_N = 4531
DEC_N = 4750
ALL_N = BASE_N + DELTA_N
BASE_B = ROOT + "/bronze_demo/historico_hasta_2025_12"
DELTA_B = ROOT + "/bronze_demo/nuevo_2026_03"
BASE_S = ROOT + "/silver/historico_hasta_2025_12"
DELTA_S = ROOT + "/silver/nuevo_2026_03"
GOLD_BASE = ROOT + "/gold/kpis_base_2025_12"
GOLD_UPDATED = ROOT + "/gold/kpis_actualizados_2026_03"
GOLD_ALERTS = ROOT + "/gold/alertas_actualizadas_2026_03"

spark = SparkSession.builder.appName("OSIPTEL_Prueba_Incremental_Aislada_V1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
jvm = sc._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(ROOT), sc._jsc.hadoopConfiguration())


def exists(path):
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def completado(path):
    return exists(path + "/_SUCCESS")


def estado_y_escribir(df, path, tipo, expected_n, etiqueta, key="fila_excel", suma="conexiones"):
    """Escritura sin overwrite; verifica conteo, IDs y suma tras releer."""
    if exists(path) and not completado(path):
        raise RuntimeError("Directorio incompleto: " + path + ". No eliminar automaticamente.")
    actual = "EXISTENTE_VERIFICADO" if exists(path) else "PUBLICADO_Y_VERIFICADO"
    if not exists(path):
        writer = df.coalesce(2).write.mode("errorifexists")
        if tipo == "csv":
            writer.option("header", "true").option("quoteAll", "true").csv(path)
        else:
            writer.parquet(path)
    if tipo == "csv":
        # Se usan exactamente los tipos STRING originales del archivo preparado.
        leido = (spark.read.option("header", "true").option("multiLine", "true")
                 .option("mode", "FAILFAST").schema(df.schema).csv(path))
    else:
        leido = spark.read.parquet(path)
    n = leido.count()
    if n != expected_n:
        raise RuntimeError(f"{etiqueta}: conteo {n} != {expected_n}")
    if key is not None:
        distintos = leido.select(key).distinct().count()
        if distintos != n:
            raise RuntimeError(f"{etiqueta}: IDs duplicados: {n} filas / {distintos} IDs")
    if suma is not None:
        suma_original = df.select(F.sum(F.col(suma).cast("decimal(38,0)"))).first()[0]
        suma_guardada = leido.select(F.sum(F.col(suma).cast("decimal(38,0)"))).first()[0]
        if suma_original != suma_guardada:
            raise RuntimeError(f"{etiqueta}: sumas distintas: {suma_original} vs {suma_guardada}")
    print(f"{etiqueta}: {actual} | filas={n} | {path}", flush=True)
    return leido


def normalizar(bronze):
    # Mantiene una fila por registro original, identificada por la fila Excel.
    silver = (bronze
        .withColumn("fila_excel", F.col("fila_excel").cast("long"))
        .withColumn("conexiones", F.col("conexiones").cast("long"))
        .withColumn("periodo", F.substring("mes", 1, 7))
        .withColumn("operador", F.upper(F.trim(F.coalesce(F.col("empresa"), F.lit("")))))
        .withColumn("zona", F.upper(F.trim(F.coalesce(F.col("departamento"), F.lit("")))))
        .withColumn("tecnologia_norm", F.upper(F.trim(F.coalesce(F.col("tecnologia"), F.lit("")))))
    )
    problemas = silver.filter(
        F.col("fila_excel").isNull() |
        F.col("conexiones").isNull() |
        (F.col("conexiones") < 0) |
        (~F.col("periodo").rlike(r"^\d{4}-\d{2}$"))
    ).count()
    if problemas:
        raise RuntimeError(f"Bronze contiene {problemas} registros sin formato apto para Silver")
    return silver


def kpis_periodo(datos, periodo):
    return (datos.filter(F.col("periodo") == periodo)
            .groupBy("periodo")
            .agg(F.sum("conexiones").cast("long").alias("total_conexiones"),
                 F.count("*").cast("long").alias("filas")))


def generar_alertas(datos):
    grup = (datos.filter(F.col("periodo").isin("2025-12", "2026-03"))
            .groupBy("operador", "zona", "periodo")
            .agg(F.sum("conexiones").cast("long").alias("total")))
    b = (grup.filter(F.col("periodo") == "2025-12")
         .select("operador", "zona", F.col("total").alias("base")))
    d = (grup.filter(F.col("periodo") == "2026-03")
         .select("operador", "zona", F.col("total").alias("actual")))
    panel = (b.join(d, ["operador", "zona"], "full")
             .fillna(0, subset=["base", "actual"])
             .withColumn("delta", F.col("actual") - F.col("base"))
             .withColumn("variacion_pct", F.when(F.col("base") > 0,
                          F.round(F.col("delta") * 100.0 / F.col("base"), 4))))
    alerta_alta = ((F.col("base") >= 10000) & (F.col("delta") <= -10000) &
                   (F.col("variacion_pct") <= -25))
    alerta_media = ((F.col("base") >= 3000) & (F.col("delta") <= -5000) &
                    (F.col("variacion_pct") <= -20))
    return (panel.withColumn("prioridad", F.when(alerta_alta, "ALTA")
                .when(alerta_media, "MEDIA"))
            .filter(F.col("prioridad").isNotNull()))


def firma_contenido(datos):
    """Compara campos originales principales sin depender de tipos de Spark."""
    campos = ("fila_excel", "mes", "conexiones", "empresa", "departamento", "tecnologia")
    cadena = F.concat_ws("\u001f", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in campos
    ])
    fila = datos.agg(
        F.count("*").alias("n"),
        F.sum(F.col("conexiones").cast("decimal(38,0)")).alias("total"),
        F.sum(F.crc32(cadena).cast("decimal(38,0)")).alias("firma")
    ).first()
    return (int(fila["n"]), int(fila["total"]), int(fila["firma"]))


def confirmar_kpis(kpis):
    valores = {r["periodo"]: (int(r["total_conexiones"]), int(r["filas"]))
               for r in kpis.collect()}
    for periodo, objetivo in {
        "2025-12": (4379544, 4750),
        "2026-03": (4135995, 4531),
    }.items():
        if periodo in valores and valores[periodo] != objetivo:
            raise RuntimeError(f"KPI {periodo} no coincide: {valores[periodo]} != {objetivo}")
    return valores


def leer_csv_bronze(path, esquema):
    if not completado(path):
        raise RuntimeError("Bronze demo ausente o incompleto: " + path)
    return (spark.read.option("header", "true").option("multiLine", "true")
            .option("mode", "FAILFAST").schema(esquema).csv(path))


def leer_silver(path, n):
    if not completado(path):
        raise RuntimeError("Silver demo ausente o incompleto: " + path)
    df = spark.read.parquet(path)
    if df.count() != n or df.select("fila_excel").distinct().count() != n:
        raise RuntimeError("Silver demo con conteo o IDs inconsistentes: " + path)
    return df


try:
    raw = (spark.read.option("header", "true").option("mode", "FAILFAST")
           .option("multiLine", "true").csv(ORIGEN))
    necesarias = {"fila_excel", "mes", "conexiones", "empresa", "departamento", "tecnologia"}
    if not necesarias.issubset(raw.columns):
        raise RuntimeError("Columnas faltantes: " + str(sorted(necesarias-set(raw.columns))))
    raw = raw.withColumn("periodo_demo", F.substring(F.col("mes"), 1, 7))
    base_raw = raw.filter(F.col("periodo_demo") < "2026-03").drop("periodo_demo")
    delta_raw = raw.filter(F.col("periodo_demo") == "2026-03").drop("periodo_demo")
    fuera = raw.filter(F.col("periodo_demo") > "2026-03").count()
    print(f"FASE={args.fase} | fuente: {ORIGEN}", flush=True)
    if fuera or base_raw.count() != BASE_N or delta_raw.count() != DELTA_N:
        raise RuntimeError("El lote origen no coincide con los periodos/filas esperados")
    if args.fase == "base":
        bb = estado_y_escribir(base_raw, BASE_B, "csv", BASE_N, "BRONZE DEMO HISTORICO", "fila_excel", "conexiones")
        ss = normalizar(bb)
        base_s = estado_y_escribir(ss, BASE_S, "parquet", BASE_N, "SILVER DEMO HISTORICO")
        kb = kpis_periodo(base_s, "2025-12")
        kb = estado_y_escribir(kb, GOLD_BASE, "parquet", 1, "GOLD DEMO BASE", None, "total_conexiones")
        print("KPI DICIEMBRE 2025:", confirmar_kpis(kb), flush=True)
    elif args.fase == "delta":
        base_s = leer_silver(BASE_S, BASE_N)
        bd = estado_y_escribir(delta_raw, DELTA_B, "csv", DELTA_N, "BRONZE DEMO LOTE NUEVO", "fila_excel", "conexiones")
        sd = normalizar(bd)
        delta_s = estado_y_escribir(sd, DELTA_S, "parquet", DELTA_N, "SILVER DEMO LOTE NUEVO")
        # Ids nuevos unicamente: nunca se reescribe la particion historica.
        repetidos = (base_s.select("fila_excel")
                     .join(delta_s.select("fila_excel"), "fila_excel", "inner")
                     .limit(1).count())
        if repetidos:
            raise RuntimeError("Se han detectado IDs que ya existian en Silver historico")
        completo = base_s.unionByName(delta_s)
        cuenta = completo.count()
        if cuenta != ALL_N:
            raise RuntimeError("Total historico+incremental incorrecto: " + str(cuenta))
        ki = kpis_periodo(completo, "2025-12").unionByName(kpis_periodo(completo, "2026-03"))
        ki = estado_y_escribir(ki, GOLD_UPDATED, "parquet", 2, "GOLD DEMO ACTUALIZADO", None, "total_conexiones")
        confirmar_kpis(ki)
        ai = generar_alertas(completo)
        n_alertas = ai.count()
        if n_alertas == 0:
            raise RuntimeError("No se generaron alertas con los umbrales de la demo")
        ai = estado_y_escribir(ai, GOLD_ALERTS, "parquet", n_alertas, "GOLD DEMO ALERTAS", None, "delta")
        print("KPI HISTORICO+NUEVO:", [r.asDict() for r in ki.orderBy("periodo").collect()], flush=True)
        print("TOP ALERTAS:", [r.asDict() for r in ai.orderBy("delta").limit(8).collect()], flush=True)
        print(f"CONTROL NO DUPLICADOS: {cuenta} registros = {BASE_N} + {DELTA_N}", flush=True)
    else:  # replay
        if not completado(BASE_B) or not completado(DELTA_B):
            raise RuntimeError("Falta Bronze de alguno de los dos lotes")
        base_s = leer_silver(BASE_S, BASE_N)
        delta_s = leer_silver(DELTA_S, DELTA_N)
        if not completado(GOLD_UPDATED) or not completado(GOLD_ALERTS):
            raise RuntimeError("Falta alguna tabla Gold del incremento")
        if base_s.select("fila_excel").join(delta_s.select("fila_excel"), "fila_excel", "inner").limit(1).count():
            raise RuntimeError("Hay IDs duplicados entre lotes")
        for etiqueta, origen_lote, silver_lote in (
            ("BASE", base_raw, base_s), ("DELTA", delta_raw, delta_s)
        ):
            firma_entrada = firma_contenido(origen_lote)
            firma_silver = firma_contenido(silver_lote)
            if firma_entrada != firma_silver:
                raise RuntimeError(
                    f"Replay {etiqueta}: contenido no coincide: {firma_entrada} vs {firma_silver}"
                )
            print(f"REPLAY {etiqueta}: firma y registros iguales {firma_entrada}", flush=True)
        total = base_s.unionByName(delta_s).count()
        if total != ALL_N:
            raise RuntimeError("La nueva ejecucion cambio el volumen: " + str(total))
        resumen = spark.read.parquet(GOLD_UPDATED)
        if resumen.count() != 2 or set(confirmar_kpis(resumen)) != {"2025-12", "2026-03"}:
            raise RuntimeError("Gold modificado tras el replay")
        if spark.read.parquet(GOLD_ALERTS).count() == 0:
            raise RuntimeError("Gold alertas quedo vacio")
        print("REPLAY: EXISTENTE_VERIFICADO | sin escrituras en Bronze, Silver ni Gold", flush=True)
        print("REGISTROS ACUMULADOS:", total, "| LOTE DELTA:", DELTA_N, flush=True)
        print("KPI:", [r.asDict() for r in resumen.orderBy("periodo").collect()], flush=True)
    print(json.dumps({"fase": args.fase, "resultado": "OK", "duracion_segundos": round(time.monotonic()-T0, 2),
                      "raiz_hdfs_demo": ROOT}, ensure_ascii=False), flush=True)
finally:
    spark.stop()
