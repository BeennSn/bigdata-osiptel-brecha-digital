#!/usr/bin/env python3
"""Publica los datasets auditados SIN convertir datos dudosos en Gold validado.

Requiere los CSV en /workspace/scripts/osiptel_cierre/inputs; unicamente crea
snapshots versionados bajo /data/mart_documental_curado. No modifica Bronze,
Silver, Gold F1 ni /data/mart_preliminar.
"""
import argparse
import hashlib
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

HDFS = 'hdfs://namenode:8020'
BASE = HDFS + '/data/mart_documental_curado'
INPUTS = Path('/workspace/scripts/osiptel_cierre/inputs')
F1 = HDFS + '/data/gold/osiptel_historico_nacional_f1/v2_b6e4155ae58e8f9106d8'
NAMES = {
    'f2': 'F2_menciones_auditadas.csv',
    'f3': 'F3_12_cifras_corregidas_y_verificadas.csv',
    'f4_narrativa': 'F4_20_indicadores_narrativa_verificados.csv',
    'f4_series': 'F4_94_series_con_estado_revision.csv',
    'f4_conflictos': 'F4_102_conflictos_para_revision.csv',
}
EXPECTED = {'f2':287,'f3':12,'f4_narrativa':20,'f4_series':94,'f4_conflictos':102}
TABLES = (
    'f2_menciones_documentales_verificadas',
    'f3_cifras_contextualizadas_verificadas',
    'f4_hogares_internet_narrativa_verificados',
    'f4_series_pendientes_revision',
    'f4_conflictos_excluidos',
    'comparacion_nacional_f1_f4_verificada',
)

def say(message):
    print('[CIERRE_OSIPTEL] '+str(message),flush=True)

def require(condition,message):
    if not condition:
        raise RuntimeError(message)

def read_csv(spark,file):
    path = INPUTS / file
    with path.open(encoding='utf-8-sig',newline='') as fd:
        import csv
        header = next(csv.reader(fd))
    schema = T.StructType([T.StructField(k,T.StringType(),True) for k in header])
    return (spark.read.option('header','true').option('enforceSchema','false')
            .option('multiLine','true').option('quote','"').option('escape','"')
            .option('mode','FAILFAST').schema(schema).csv(path.as_uri()))

def sha_check():
    meta=json.loads((INPUTS/'manifest_archivos.json').read_text(encoding='utf8'))
    for file in NAMES.values():
        expected=meta['files'][file]['sha256']
        real=hashlib.sha256((INPUTS/file).read_bytes()).hexdigest()
        require(real==expected,'CSV modificado: '+file)
    version_content = ('curaduria_v1_20261126|'+F1+'|'+json.dumps(
        {k:meta['files'][v]['sha256'] for k,v in sorted(NAMES.items())},sort_keys=True))
    return hashlib.sha256(version_content.encode('utf8')).hexdigest()[:20]

def fingerprint(df,label):
    cols=sorted(df.columns)
    rows=df.select(*cols).toJSON().collect()
    require(len(rows)<=2000,'Tabla demasiado grande para firma exacta: '+label)
    schema=sorted((x.name,x.dataType.simpleString()) for x in df.schema.fields)
    data='\n'.join(sorted(rows)).encode('utf8')
    return {'filas':len(rows),'schema':schema,'sha256_contenido':hashlib.sha256(data).hexdigest()}

def publish(spark,name,df,version):
    base=BASE+'/'+name
    final=base+'/v1_'+version
    stage=base+'/_staging_v1_'+version
    PathH=spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    fs=PathH(base).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    require(fs.exists(PathH(base)),'Directorio HDFS no preparado: '+base)
    expected=fingerprint(df,name)
    require(expected['filas']>0,'Tabla vacia '+name)
    if fs.exists(PathH(final)):
        actual=fingerprint(spark.read.parquet(final),name)
        require(actual==expected,'Snapshot existente diferente: '+final)
        state='EXISTENTE_VERIFICADO'
    else:
        require(not fs.exists(PathH(stage)),'Staging preexistente; inspeccionar manualmente: '+stage)
        say('Escribiendo '+name+' registros='+str(expected['filas']))
        df.coalesce(1).write.mode('errorifexists').option('compression','snappy').parquet(stage)
        require(fingerprint(spark.read.parquet(stage),name)==expected,'Falló integridad en staging: '+name)
        require(fs.rename(PathH(stage),PathH(final)),'No se pudo publicar HDFS: '+name)
        require(fingerprint(spark.read.parquet(final),name)==expected,'Falló integridad final: '+name)
        state='PUBLICADO_Y_VERIFICADO'
    say('OK '+name+' '+state+' filas='+str(expected['filas']))
    return dict(tabla=name,estado=state,filas=expected['filas'],ruta_hdfs=final,sha256_contenido=expected['sha256_contenido'])

def main():
    p=argparse.ArgumentParser()
    group=p.add_mutually_exclusive_group(required=True)
    group.add_argument('--preflight',action='store_true')
    group.add_argument('--publish',action='store_true')
    args=p.parse_args()
    ver=sha_check()
    spark=(SparkSession.builder.appName('OSIPTEL_Cierre_Documental_Curado_v1')
           .master('local[2]').config('spark.hadoop.fs.defaultFS',HDFS)
           .config('spark.sql.shuffle.partitions','4').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        src={k:read_csv(spark,filename) for k,filename in NAMES.items()}
        counts={k:df.count() for k,df in src.items()}
        require(all(counts[k]==n for k,n in EXPECTED.items()),'Conteos de CSV distintos: '+str(counts))
        gold=spark.read.parquet(F1)
        require(gold.count()==71,'Gold F1 no tiene 71 periodos')
        for k in ('f2','f3','f4_narrativa','f4_series','f4_conflictos'):
            require(src[k].filter(F.col(src[k].columns[0]).isNull()).count()==0,'Clave nula en '+k)
        # F2: evidencia documental verificada; legalmente NO se asume mandato concedido.
        f2=(src['f2'].withColumn('estado_curaduria',F.lit('MENCION_DOCUMENTAL_VERIFICADA'))
            .withColumn('aprobacion_institucional',F.lit('NO_CONSTA'))
            .withColumn('uso_permitido',F.lit('CATALOGO_DE_MENCIONES; NO_CONTAR_COMO_MANDATOS_APROBADOS')))
        require(f2.filter((F.col('verificacion_sha256')!='SI') | (F.col('pagina_existe')!='SI') |
                          (~F.col('termino_en_pagina').isin('SI','SI_PDFTOTEXT')) |
                          (F.col('apto_como_mandato_aprobado')!='NO')).count()==0,'F2 tiene registros improcedentes')
        # F3: valores y contexto contrastados con originales; no confundir variaciones y stocks.
        f3=(src['f3'].withColumn('valor',F.col('valor').cast('double'))
            .withColumn('registro',F.col('registro').cast('int'))
            .withColumn('estado_curaduria',F.lit('VALOR_Y_CONTEXTO_VERIFICADOS_EN_PDF'))
            .withColumn('aprobacion_institucional',F.lit('NO_CONSTA')))
        require(f3.filter(F.col('valor').isNull() | F.col('periodo_validado').isNull() |
                          (F.col('sha256_igual_original')!='SI') | (F.col('valor_en_pagina')!='SI')).count()==0,'F3 incompleto')
        # F4: selección conservadora de 20 indicadores explícitos; otros 74 y 102 separados.
        f4=(src['f4_narrativa'].withColumn('porcentaje',F.col('porcentaje').cast('double'))
            .withColumn('pagina_pdf',F.col('pagina_pdf').cast('int'))
            .withColumn('estado_curaduria',F.lit('VERIFICADO_EN_NARRATIVA_PDF'))
            .withColumn('aprobacion_institucional',F.lit('NO_CONSTA')))
        require(f4.filter(F.col('porcentaje').isNull() | (~F.col('porcentaje').between(0,100)) |
                          (F.col('evidencia_periodo_y_area')!='SI')).count()==0,'F4 indicadores fuera de rango o sin evidencia')
        pending=(src['f4_series']
                 .filter(F.col('estado_revision')=='PENDIENTE_VALIDACION_SEMANTICA_DE_TABLA')
                 .withColumn('uso_permitido',F.lit('EXPLORACION_CON_ADVERTENCIA; NO_PARA_INDICADORES_DEFINITIVOS')))
        conflicts=(src['f4_conflictos'].withColumn('indicador',F.lit('Hogares con acceso a Internet'))
                   .withColumn('uso_permitido',F.lit('EXCLUIDO_DE_INDICADORES_DEFINITIVOS')))
        require(pending.count()==74 and conflicts.count()==102,'F4 pendientes y conflictos no coinciden')
        require(f4.select('periodo','ambito').distinct().count()==20,'F4 contiene claves duplicadas')
        # F1 nacional: stocks de cierre trimestral, no suma de varios meses.
        q=F.substring(F.col('periodo'),6,2).cast('int')
        f1=(gold.filter(q.isin(3,6,9,12))
            .select(F.concat(F.substring('periodo',1,4),F.lit('-T'),
                             (F.floor((q-1)/3)+1).cast('int').cast('string')).alias('periodo_trimestre'),
                    F.col('periodo').alias('periodo_cierre_f1'),
                    F.col('conexiones_fijas').alias('conexiones_fijas_f1'),
                    F.col('origen_sha256').alias('sha256_excel_f1')))
        f4n=(f4.filter(F.col('ambito')=='Nacional')
              .select(F.col('periodo').alias('periodo_trimestre'),
                      F.col('porcentaje').alias('hogares_internet_pct_f4'),
                      F.col('pdf').alias('pdf_inei'),F.col('pagina_pdf').alias('pagina_pdf_inei'),
                      F.col('sha256_pdf').alias('sha256_pdf_inei')))
        require(f4n.count()==5,'F4 nacional debe tener 5 trimestres verificados')
        comparison=(f4n.join(f1,'periodo_trimestre','left')
             .withColumn('estado_cobertura',F.when(F.col('conexiones_fijas_f1').isNull(),'SOLO_INEI')
                         .otherwise('AMBAS_FUENTES'))
             .withColumn('tipo_comparacion',F.lit('DESCRIPTIVA_NO_CAUSAL'))
             .withColumn('nota',F.lit('OSIPTEL: numero de conexiones fijas. INEI: porcentaje de hogares con algun acceso a Internet. No equivalentes, no restar valores ni interpretar como brecha causal.'))
             .withColumn('estado_curaduria',F.lit('INDICADOR_F4_VERIFICADO_EN_PDF_Y_GOLD_F1_VERIFICADO'))
             .withColumn('aprobacion_institucional',F.lit('NO_CONSTA')))
        require(comparison.count()==5 and comparison.filter(F.col('estado_cobertura')=='AMBAS_FUENTES').count()==4,
                'Cobertura esperada 4 trimestres comparables y 1 solo INEI')
        require(comparison.filter((F.col('periodo_trimestre')=='2026-T2') &
                    (F.col('estado_cobertura')!='SOLO_INEI')).count()==0,'2026-T2 debe carecer de F1')
        tables=dict(zip(TABLES,[f2,f3,f4,pending,conflicts,comparison]))
        say('CONTROLES_OK '+json.dumps({'F1_gold_periodos':71,'F2':287,'F3':12,'F4_verificados':20,'F4_pendientes':74,
                                       'F4_conflictos':102,'comparacion_nacional':5,'comparacion_con_F1':4,'version':'v1_'+ver},ensure_ascii=False))
        report={'modo':'PUBLICADO' if args.publish else 'PREFLIGHT','version':'v1_'+ver,
                'nivel':'CURADURIA_TECNICO_DOCUMENTAL; NO CERTIFICACION INSTITUCIONAL',
                'source_F1':F1,'datos_revisados':counts,'tablas':list(tables)}
        if args.publish:
            report['publicaciones']=[publish(spark,k,df,ver) for k,df in tables.items()]
        print('=== RESUMEN CIERRE CURADO OSIPTEL ===',flush=True)
        print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    finally:
        spark.stop()

if __name__=='__main__':
    main()
