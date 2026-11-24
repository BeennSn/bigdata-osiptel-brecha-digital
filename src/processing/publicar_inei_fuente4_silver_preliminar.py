#!/usr/bin/env python3
"""Publica 3 tablas INEI de Silver preliminar, inmutables e idempotentes.

Entradas: archivos producidos por depurar_inei_fuente4.py, copiados a
/workspace/scripts/inputs/inei_fuente4/. No aprueba cifras para Gold.
"""
import csv
import hashlib
import json
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

INP = Path('/workspace/scripts/inputs/inei_fuente4')
HDFS = 'hdfs://namenode:8020'
FILES = {
    'trazables': 'indicadores_trazables.csv',
    'coherentes': 'indicadores_coherentes_preliminares.csv',
    'conflictos': 'conflictos_para_revision.csv',
    'invalidos': 'registros_invalidos.csv',
    'resumen': 'resumen.json',
}
TABLES = {
    'trazables': 'inei_indicadores_trazables_fuente4',
    'coherentes': 'inei_series_sin_conflicto_fuente4',
    'conflictos': 'inei_conflictos_fuente4',
}
HEX = re.compile(r'^[0-9a-f]{64}$')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def readcsv(filename):
    path = INP / filename
    if not path.is_file():
        raise RuntimeError('Archivo de entrada ausente: ' + str(path))
    with path.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        if not columns or len(columns) != len(set(columns)):
            raise RuntimeError('Cabecera invalida: ' + filename)
        rows = list(reader)
        if any(None in r for r in rows):
            raise RuntimeError('Columnas de más en ' + filename)
    return columns, rows


def key(row):
    return (row['periodo'], row['ambito'], row['indicador'])


def precontrol():
    rows = {}
    columns = {}
    for k in ('trazables', 'coherentes', 'conflictos', 'invalidos'):
        columns[k], rows[k] = readcsv(FILES[k])
    meta = json.loads((INP / FILES['resumen']).read_text(encoding='utf-8'))
    if rows['invalidos'] or meta['registros_invalidos'] != 0:
        raise RuntimeError('Existen registros invalidos: prohibida publicacion automatica')
    if not meta.get('coincide_con_reporte_anterior') or int(meta['pdf_originales_verificados']) != 89:
        raise RuntimeError('Auditoria INEI previa no coincide')
    if len(rows['trazables']) != meta['registros_validos_en_formato']:
        raise RuntimeError('Conteo de registros trazables inconsistente')
    if len(rows['coherentes']) != meta['series_sin_conflicto'] or len(rows['conflictos']) != meta['grupos_con_conflicto']:
        raise RuntimeError('Conteos de grupos inconsistentes')
    bykey = defaultdict(list)
    for r in rows['trazables']:
        if not HEX.fullmatch(r['sha256_pdf']) or not r['evidencia'].strip():
            raise RuntimeError('Falta SHA o evidencia de un registro')
        if not re.fullmatch(r'\d{4}-T[1-4]', r['periodo']) or not r['ambito'].strip():
            raise RuntimeError('Periodo/ambito invalido')
        if not 0 <= Decimal(r['porcentaje'].replace(',', '.')) <= 100:
            raise RuntimeError('Porcentaje fuera de rango')
        if r['apto_gold'] != 'NO' or int(r['pagina_pdf']) < 1:
            raise RuntimeError('Aprobacion indebida o pagina invalida')
        bykey[key(r)].append(r)
    ok = {k for k, group in bykey.items() if len({Decimal(r['porcentaje']) for r in group}) == 1}
    conflict = set(bykey) - ok
    if len(ok) != len(rows['coherentes']) or len(conflict) != len(rows['conflictos']):
        raise RuntimeError('Particion por conflictos inconsistente')
    for r in rows['trazables']:
        expected = 'SIN_CONFLICTO_DOCUMENTAL_POR_REVISAR' if key(r) in ok else 'CONFLICTO_ENTRE_PUBLICACIONES'
        if r['estado_validacion'] != expected:
            raise RuntimeError('Estado invalido en indicador trazable')
    seen = set()
    for r in rows['coherentes']:
        k = key(r)
        if k not in ok or k in seen or r['apto_gold'] != 'NO' or r['estado_validacion'] != 'SIN_CONFLICTO_DOCUMENTAL_POR_REVISAR':
            raise RuntimeError('Grupo coherente inesperado o duplicado')
        seen.add(k)
        group = bykey[k]
        if Decimal(r['porcentaje']) != Decimal(group[0]['porcentaje']):
            raise RuntimeError('Valor del grupo coherente no coincide')
        if int(r['numero_evidencias']) != len(group) or int(r['numero_documentos']) != len({v['documento'] for v in group}):
            raise RuntimeError('Proveniencia de serie coherente incompleta')
    seen = set()
    for r in rows['conflictos']:
        k = key(r)
        if k not in conflict or k in seen or r['apto_gold'] != 'NO' or r['estado_validacion'] != 'CONFLICTO_ENTRE_PUBLICACIONES':
            raise RuntimeError('Grupo conflictivo inesperado o duplicado')
        seen.add(k)
        group = bykey[k]
        if int(r['registros']) != len(group):
            raise RuntimeError('Conteo de evidencias en conflicto inconsistente')
        if {Decimal(x) for x in r['valores'].split(';')} != {Decimal(x['porcentaje']) for x in group}:
            raise RuntimeError('Valores de conflicto distintos de evidencias')
        if set(r['documentos'].split(';')) != {x['documento'] for x in group}:
            raise RuntimeError('Documentos de conflicto no coinciden')
    if sum(len(bykey[k]) for k in conflict) != meta['registros_afectados_por_conflictos']:
        raise RuntimeError('Conteo de registros en conflictos no coincide')
    package = hashlib.sha256(''.join(FILES[k] + ':' + digest(INP / FILES[k]) + '\n' for k in FILES).encode('utf-8')).hexdigest()
    print('PRECONTROL INEI:', json.dumps({
        'pdf_verificados': meta['pdf_originales_verificados'],
        'trazables':len(rows['trazables']), 'series_sin_conflicto':len(ok),
        'grupos_conflictivos':len(conflict), 'afectados_por_conflictos':meta['registros_afectados_por_conflictos'],
        'sha_paquete':package}, ensure_ascii=False), flush=True)
    return columns, rows, meta, package


def verificar(df, expected, sha_csv, package, grupo):
    if df.count() != expected:
        raise RuntimeError('Perdida de filas en ' + grupo)
    if df.filter(F.col('estado_curaduria') != 'PRELIMINAR_NO_APTO_PARA_GOLD').count():
        raise RuntimeError('Estado de curaduria inesperado: ' + grupo)
    if df.filter(F.col('apto_gold') != 'NO').count():
        raise RuntimeError('Datos sin restriccion Gold: ' + grupo)
    if df.filter(F.col('periodo').isNull() | F.col('ambito').isNull() | F.col('indicador').isNull() | F.col('estado_validacion').isNull()).count():
        raise RuntimeError('Nulos criticos en ' + grupo)
    if {v[0] for v in df.select('sha256_archivo_csv').distinct().collect()} != {sha_csv}:
        raise RuntimeError('SHA CSV inconsistente en ' + grupo)
    if {v[0] for v in df.select('sha256_paquete').distinct().collect()} != {package}:
        raise RuntimeError('SHA paquete inconsistente en ' + grupo)
    if grupo != 'trazables' and df.select('periodo','ambito','indicador').distinct().count() != expected:
        raise RuntimeError('Grupos duplicados en ' + grupo)
    if grupo != 'conflictos':
        if df.filter(F.col('porcentaje').isNull() | (F.col('porcentaje') < 0) | (F.col('porcentaje') > 100) | F.col('pagina_pdf').isNull()).count():
            raise RuntimeError('Dato numerico o pagina invalida en ' + grupo)


def publicar(spark, grupo, columns, rows, package):
    name = FILES[grupo]
    src = INP / name
    sha_csv = digest(src)
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in columns])
    df = (spark.read.option('header','true').option('enforceSchema','false')
          .option('multiLine','true').option('escape','"').option('mode','FAILFAST')
          .schema(schema).csv(str(src)))
    df = (df.withColumn('estado_curaduria', F.lit('PRELIMINAR_NO_APTO_PARA_GOLD'))
          .withColumn('sha256_archivo_csv', F.lit(sha_csv))
          .withColumn('sha256_paquete', F.lit(package))
          .withColumn('fuente', F.lit('FUENTE_4_INEI')))
    if grupo != 'conflictos':
        df = df.withColumn('porcentaje', F.col('porcentaje').cast('double')).withColumn('pagina_pdf', F.col('pagina_pdf').cast('int'))
    if grupo == 'coherentes':
        df = df.withColumn('numero_evidencias', F.col('numero_evidencias').cast('int')).withColumn('numero_documentos', F.col('numero_documentos').cast('int'))
    if grupo == 'conflictos':
        df = df.withColumn('registros', F.col('registros').cast('int'))
    verificar(df, len(rows), sha_csv, package, grupo)
    base = HDFS + '/data/silver/' + TABLES[grupo]
    version = 'v1_' + package[:20]
    target, staging = base+'/'+version, base+'/_staging_'+version
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(HDFS), spark._jsc.hadoopConfiguration())
    PathH = jvm.org.apache.hadoop.fs.Path
    final_path, stage_path = PathH(target), PathH(staging)
    if fs.exists(final_path):
        verificar(spark.read.parquet(target), len(rows), sha_csv, package, grupo)
        print('SNAPSHOT EXISTENTE VERIFICADO:', target, flush=True)
        status = 'EXISTENTE_VERIFICADO'
    else:
        if fs.exists(stage_path):
            raise RuntimeError('Staging preexistente; inspeccionar: '+staging)
        df.coalesce(1).write.mode('errorifexists').parquet(staging)
        verificar(spark.read.parquet(staging), len(rows), sha_csv, package, grupo)
        if not fs.rename(stage_path, final_path):
            raise RuntimeError('No se pudo mover staging a destino: '+target)
        verificar(spark.read.parquet(target), len(rows), sha_csv, package, grupo)
        status = 'PUBLICADO_Y_VERIFICADO'
    return {'tabla':TABLES[grupo], 'estado':status, 'filas':len(rows), 'ruta_hdfs':target}


def main():
    columns, rows, meta, package = precontrol()
    spark = SparkSession.builder.appName('OSIPTEL_INEI_Fuente4_Silver_Preliminar_V1').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        out = {k: publicar(spark, k, columns[k], rows[k], package) for k in ('trazables','coherentes','conflictos')}
        print('\n=== SILVER INEI FUENTE 4 ===')
        print(json.dumps({'sha_paquete':package, 'pdf_verificados':meta['pdf_originales_verificados'],
                          'publicaciones':out, 'restriccion':'NINGUNA TABLA QUEDA APROBADA PARA GOLD; 102 GRUPOS REQUIEREN REVISION'},
                         indent=2, ensure_ascii=False), flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
