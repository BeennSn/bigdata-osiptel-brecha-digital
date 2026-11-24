#!/usr/bin/env python3
"""Publica observaciones documentales PDF en Silver preliminar (no Gold).

Entrada: CSV de extraer_hechos_fuentes_2_3_v1.py y auditoria.csv.
Valida integridad entre auditoria y candidatos; no infiere decisiones ni periodos.
Escritura HDFS inmutable e idempotente usando SHA256 de los tres CSV.
Uso: spark-submit --master 'local[2]' /workspace/scripts/publicar_silver_candidatos_pdf_2_3.py
"""
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

INPUT = Path('/workspace/scripts/inputs')
AUD = INPUT / 'auditoria_pdf_2_3.csv'
REG = INPUT / 'fuente2_hechos_regulatorios_candidatos.csv'
EST = INPUT / 'fuente3_indicadores_estadisticos_candidatos.csv'
HDFS = 'hdfs://namenode:8020'
FIELDS_AUD = ['fuente', 'documento', 'sha256', 'paginas', 'registros_candidatos', 'estado', 'error']
FIELDS_REG = ['documento', 'sha256', 'pagina_pdf', 'linea_pdf', 'tipo_hecho', 'estado_validacion', 'evidencia']
FIELDS_EST = ['documento', 'sha256', 'pagina_pdf', 'linea_pdf', 'indicador_candidato', 'valor', 'unidad', 'ambito_mencionado', 'periodo_verificado', 'estado_validacion', 'evidencia']
HEX = re.compile(r'^[a-f0-9]{64}$')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda: f.read(1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()


def leer(path, campos):
    if not path.is_file():
        raise RuntimeError('Falta entrada: ' + str(path))
    with path.open(newline='', encoding='utf-8-sig') as f:
        lector = csv.DictReader(f)
        if lector.fieldnames != campos:
            raise RuntimeError('Cabecera inesperada en ' + str(path) + ': ' + str(lector.fieldnames))
        rows = list(lector)
    return rows


def preparar_auditoria():
    rows = leer(AUD, FIELDS_AUD)
    index = {}
    for r in rows:
        key = (r['fuente'], r['documento'])
        if key in index:
            raise RuntimeError('Documento duplicado en auditoria: ' + str(key))
        if not HEX.fullmatch(r['sha256']):
            raise RuntimeError('SHA invalido auditoria: ' + str(key))
        if r['error'] or r['estado'] == 'ERROR_LECTURA':
            raise RuntimeError('PDF con error de lectura en auditoria: ' + str(key))
        if int(r['paginas']) < 1 or int(r['registros_candidatos']) < 0:
            raise RuntimeError('Conteos invalidos en auditoria: ' + str(key))
        if r['fuente'] not in ('FUENTE_2', 'FUENTE_3'):
            raise RuntimeError('Fuente inesperada en auditoria')
        index[key] = r
    if not any(k[0] == 'FUENTE_2' for k in index) or not any(k[0] == 'FUENTE_3' for k in index):
        raise RuntimeError('Auditoria incompleta: faltan fuentes')
    return rows, index


def validar_registros(rows, source, index):
    counts = Counter()
    for r in rows:
        key = (source, r['documento'])
        meta = index.get(key)
        if meta is None:
            raise RuntimeError('Documento no auditado: ' + str(key))
        if r['sha256'] != meta['sha256']:
            raise RuntimeError('SHA no coincide con auditoria: ' + str(key))
        pag = int(r['pagina_pdf'])
        if not 1 <= pag <= int(meta['paginas']):
            raise RuntimeError('Pagina fuera del PDF: ' + str(key) + ' ' + str(pag))
        if not r['evidencia'].strip() or not r['estado_validacion'].strip():
            raise RuntimeError('Registro sin evidencia o estado: ' + str(key))
        if source == 'FUENTE_2':
            if int(r['linea_pdf']) < 1 or not r['tipo_hecho']:
                raise RuntimeError('Registro regulatorio incompleto: ' + str(key))
            if 'NO_EQUIVALE_A_MANDATO_APROBADO' not in r['estado_validacion']:
                raise RuntimeError('Estado regulatorio inesperado: ' + str(key))
        else:
            if not r['indicador_candidato'] or not r['unidad']:
                raise RuntimeError('Indicador sin tipo/unidad: ' + str(key))
            try:
                val = float(r['valor'])
            except ValueError as e:
                raise RuntimeError('Valor no numerico: ' + str(key)) from e
            if not 0 <= val < 1e13:
                raise RuntimeError('Valor fuera de rango: ' + str(key))
            if r['unidad'] == 'porcentaje' and val > 100:
                raise RuntimeError('Porcentaje fuera de rango: ' + str(key))
        counts[key] += 1
    for key, meta in index.items():
        if key[0] == source and counts[key] != int(meta['registros_candidatos']):
            raise RuntimeError('Conteo no coincide auditoria: ' + str(key) + ' ' + str(counts[key]))


def verificar_df(df, n, digest_archivo):
    filas = df.count()
    if filas != n:
        raise RuntimeError('Conteo Spark: esperado %s; obtenido %s' % (n, filas))
    if df.filter(F.col('documento').isNull() | F.col('pagina_pdf').isNull() | F.col('evidencia').isNull() | F.col('id_evidencia').isNull()).count():
        raise RuntimeError('Hay nulos criticos')
    if df.select('id_evidencia').distinct().count() != n:
        raise RuntimeError('Existen evidencias duplicadas')
    hashes = [x[0] for x in df.select('sha256_archivo_csv').distinct().collect()]
    if hashes != [digest_archivo]:
        raise RuntimeError('SHA entrada no coincide')
    return filas


def publicar(spark, ruta_local, campos, fuente, tabla, tipo, expected, paquete):
    from pyspark.sql.functions import col

    schema = T.StructType([T.StructField(k, T.StringType(), True) for k in campos])
    df = (spark.read.option('header', 'true')
          .option('enforceSchema', 'false')
          .option('multiLine', 'true')
          .option('escape', '"')
          .option('mode', 'FAILFAST')
          .schema(schema)
          .csv(str(ruta_local)))

    sha_csv = digest(ruta_local)
    # Valores y metadatos conservados con semantica explicita de candidatura.
    df = (df.withColumn('pagina_pdf', col('pagina_pdf').cast('int'))
          .withColumn('linea_pdf', col('linea_pdf').cast('int'))
          .withColumn('fuente', F.lit(fuente))
          .withColumn('estado_curaduria', F.lit('PRELIMINAR_NO_APROBADO_PARA_GOLD'))
          .withColumn('sha256_archivo_csv', F.lit(sha_csv))
          .withColumn('sha256_paquete', F.lit(paquete))
          .withColumn('id_evidencia', F.sha2(F.concat_ws('|',
                      F.lit(fuente), col('sha256'), col('pagina_pdf').cast('string'),
                      F.coalesce(col('linea_pdf').cast('string'), F.lit('')),
                      col(tipo), col('evidencia')), 256)))
    if fuente == 'FUENTE_3':
        df = df.withColumn('valor', col('valor').cast('double'))
        if df.filter(F.col('valor').isNull()).count():
            raise RuntimeError('Conversion numerica fallida')
    verificar_df(df, expected, sha_csv)

    base = HDFS + '/data/silver/' + tabla
    ver = 'v1_' + paquete[:20]
    destino = base + '/' + ver
    staging = base + '/_staging_' + ver
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(HDFS), conf)
    PathH = jvm.org.apache.hadoop.fs.Path
    final_path = PathH(destino)
    stage_path = PathH(staging)
    if fs.exists(final_path):
        verificar_df(spark.read.parquet(destino), expected, sha_csv)
        print('SNAPSHOT EXISTENTE VERIFICADO:', destino, flush=True)
        return {'tabla': tabla, 'estado': 'EXISTENTE_VERIFICADO', 'filas': expected, 'ruta': destino}
    if fs.exists(stage_path):
        raise RuntimeError('Staging preexistente; inspeccionar manualmente antes de continuar: ' + staging)
    (df.coalesce(1).write.mode('errorifexists').parquet(staging))
    verificar_df(spark.read.parquet(staging), expected, sha_csv)
    if not fs.rename(stage_path, final_path):
        raise RuntimeError('No se pudo publicar staging HDFS a ' + destino)
    verificar_df(spark.read.parquet(destino), expected, sha_csv)
    return {'tabla': tabla, 'estado': 'PUBLICADO_Y_VERIFICADO', 'filas': expected, 'ruta': destino}


def main():
    audit, index = preparar_auditoria()
    regs = leer(REG, FIELDS_REG)
    ests = leer(EST, FIELDS_EST)
    validar_registros(regs, 'FUENTE_2', index)
    validar_registros(ests, 'FUENTE_3', index)
    if not regs or not ests:
        raise RuntimeError('Alguna fuente no contiene candidatos')
    paquete = hashlib.sha256((digest(AUD) + digest(REG) + digest(EST)).encode('ascii')).hexdigest()
    print('PRECONTROL:', json.dumps({'pdf_auditados':len(audit), 'candidatos_fuente2':len(regs), 'candidatos_fuente3':len(ests), 'sha_paquete':paquete}, ensure_ascii=False), flush=True)
    spark = SparkSession.builder.appName('OSIPTEL_Silver_PDF_Candidatos_V1').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        a = publicar(spark, REG, FIELDS_REG, 'FUENTE_2', 'menciones_regulatorias_pdf_fuente2', 'tipo_hecho', len(regs), paquete)
        b = publicar(spark, EST, FIELDS_EST, 'FUENTE_3', 'cifras_candidatas_pdf_fuente3', 'indicador_candidato', len(ests), paquete)
        print('\n=== SILVER PDF PRELIMINAR ===')
        print(json.dumps({'paquete':paquete, 'pdf_auditados':len(audit), 'fuente2':a, 'fuente3':b, 'restriccion':'NO_USAR_EN_GOLD_SIN_VALIDAR_RESULTADO_REGULATORIO_Y_PERIODO_AMBITO_ESTADISTICO'}, indent=2, ensure_ascii=False), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
