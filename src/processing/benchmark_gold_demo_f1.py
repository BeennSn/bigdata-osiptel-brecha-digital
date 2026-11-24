#!/usr/bin/env python3
"""Benchmark de recálculo completo vs actualización con estado agregado previo.
Solo lee snapshots HDFS existentes. No publica ni sobrescribe datos.
"""
import argparse
import json
import statistics
import time
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

p = argparse.ArgumentParser()
p.add_argument('--reps', type=int, default=3)
args = p.parse_args()
if not 1 <= args.reps <= 10:
    raise SystemExit('--reps debe estar entre 1 y 10')

ROOT = 'hdfs://namenode:8020/data/demo_incremental_osiptel_v1'
K = ['periodo', 'total_conexiones', 'filas']
A = ['operador', 'zona', 'base', 'actual', 'delta', 'variacion_pct', 'prioridad']
spark = SparkSession.builder.appName('OSIPTEL_Benchmark_Gold_Demo_F1').master('local[2]').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


def filas_ordenadas(df, campos):
    registros = [{c: r[c] for c in campos} for r in df.select(*campos).collect()]
    return sorted(registros, key=lambda r: json.dumps(r, ensure_ascii=False, sort_keys=True, default=str))


def kpis(df):
    return (df.groupBy('periodo')
            .agg(F.sum('conexiones').cast('long').alias('total_conexiones'),
                 F.count('*').cast('long').alias('filas')))


def grupos(df):
    return (df.groupBy('operador', 'zona', 'periodo')
            .agg(F.sum('conexiones').cast('long').alias('total')))


def alertas(grup):
    b = (grup.filter(F.col('periodo') == '2025-12')
         .select('operador', 'zona', F.col('total').alias('base')))
    d = (grup.filter(F.col('periodo') == '2026-03')
         .select('operador', 'zona', F.col('total').alias('actual')))
    panel = (b.join(d, ['operador', 'zona'], 'full')
             .fillna(0, subset=['base', 'actual'])
             .withColumn('delta', F.col('actual') - F.col('base'))
             .withColumn('variacion_pct', F.when(F.col('base') > 0,
                         F.round(F.col('delta') * 100.0 / F.col('base'), 4))))
    alta = ((F.col('base') >= 10000) & (F.col('delta') <= -10000) &
            (F.col('variacion_pct') <= -25))
    media = ((F.col('base') >= 3000) & (F.col('delta') <= -5000) &
             (F.col('variacion_pct') <= -20))
    return (panel.withColumn('prioridad', F.when(alta, 'ALTA').when(media, 'MEDIA'))
            .filter(F.col('prioridad').isNotNull()))


def obtener(k, a):
    return {'kpis': filas_ordenadas(k, K), 'alertas': filas_ordenadas(a, A)}


try:
    base = spark.read.parquet(ROOT + '/silver/historico_hasta_2025_12')
    nuevo = spark.read.parquet(ROOT + '/silver/nuevo_2026_03')
    kbase = spark.read.parquet(ROOT + '/gold/kpis_base_2025_12').select(*K).persist(StorageLevel.MEMORY_AND_DISK)
    gbase = (grupos(base.filter(F.col('periodo') == '2025-12'))
             .persist(StorageLevel.MEMORY_AND_DISK))

    # Preparación de estado histórico: se mide por separado, fuera de cada actualización.
    t_preparacion = time.perf_counter()
    base_n, nuevo_n = base.count(), nuevo.count()
    gbase_n = gbase.count()
    base_kpi_n = kbase.count()
    preparacion_s = round(time.perf_counter() - t_preparacion, 3)
    if (base_n, nuevo_n, base_kpi_n) != (136575, 4531, 1):
        raise RuntimeError(f'Snapshots inesperados: {base_n}, {nuevo_n}, {base_kpi_n}')

    def recalculo_completo():
        datos = base.unionByName(nuevo).filter(F.col('periodo').isin('2025-12', '2026-03'))
        return obtener(kpis(datos), alertas(grupos(datos)))

    def actualizacion_incremental():
        # El histórico agregado y su KPI ya existen antes del inicio de la actualización.
        agrupado = gbase.unionByName(grupos(nuevo))
        return obtener(kbase.unionByName(kpis(nuevo)), alertas(agrupado))

    funciones = {'completo': recalculo_completo, 'incremental_con_estado': actualizacion_incremental}
    referencia = recalculo_completo()  # Calentamiento fuera de las mediciones.
    segundo = actualizacion_incremental()
    publicado = obtener(
        spark.read.parquet(ROOT + '/gold/kpis_actualizados_2026_03'),
        spark.read.parquet(ROOT + '/gold/alertas_actualizadas_2026_03')
    )
    if not (referencia == segundo == publicado):
        raise RuntimeError('Los dos métodos no coinciden entre sí o con el Gold publicado')
    esperado = {'2025-12': (4379544, 4750), '2026-03': (4135995, 4531)}
    kpi_visto = {r['periodo']: (r['total_conexiones'], r['filas']) for r in referencia['kpis']}
    if kpi_visto != esperado or len(referencia['alertas']) != 17:
        raise RuntimeError('Los KPI o las 17 alertas no coinciden con la demostración')

    tiempos = {'completo': [], 'incremental_con_estado': []}
    for i in range(args.reps):
        orden = ('completo', 'incremental_con_estado') if i % 2 == 0 else ('incremental_con_estado', 'completo')
        for metodo in orden:
            inicio = time.perf_counter()
            salida = funciones[metodo]()
            transcurrido = round(time.perf_counter() - inicio, 3)
            if salida != publicado:
                raise RuntimeError(f'Resultado inconsistente en repetición {i+1}: {metodo}')
            tiempos[metodo].append(transcurrido)

    mc = statistics.median(tiempos['completo'])
    mi = statistics.median(tiempos['incremental_con_estado'])
    reporte = {
        'estado': 'OK', 'repeticiones': args.reps,
        'registros_historicos': base_n, 'registros_delta': nuevo_n,
        'grupos_historicos_preparados': gbase_n,
        'tiempo_preparacion_estado_historico_segundos': preparacion_s,
        'kpis': referencia['kpis'], 'alertas_verificadas': len(referencia['alertas']),
        'equivalencia_con_gold_demo_publicado': True,
        'tiempos_segundos': tiempos,
        'mediana_completo_segundos': round(mc, 3),
        'mediana_incremental_segundos': round(mi, 3),
        'razon_mediana_completo_sobre_incremental': round(mc / mi, 3) if mi else None,
        'aclaracion': 'Comparación de cálculo Gold con estado histórico preagregado; no es duración end-to-end ni benchmark de la demo incremental actualmente desplegada. Solo lectura HDFS.'
    }
    print('=== RESULTADO BENCHMARK GOLD ===', flush=True)
    print(json.dumps(reporte, ensure_ascii=False, indent=2), flush=True)
finally:
    spark.stop()
