#!/usr/bin/env python3
"""Publica un PDF nuevo de forma inmutable en HDFS: RAW, Silver y Gold documental separado.

El Gold se crea solamente si se aporta un dictamen de revisión explícito y verificable.
No toca los Gold de F1 ni los mart de F2/F3/F4 publicados anteriormente.
"""
import argparse
import hashlib
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

HDFS='hdfs://namenode:8020'
RAW=HDFS+'/data/raw/pdf_controlados_v1'
SILVER=HDFS+'/data/silver/pdf_nuevos_candidatos_v1'
GOLD=HDFS+'/data/gold/pdf_documental_revisado_v1'
FIELDS=['pdf_sha256','archivo_original','fuente','clasificacion','pagina','tipo','accion','fragmento','estado','valor','unidad','periodo','ambito','revisor','justificacion','evidencia_sha256']

def require(b,message):
    if not b: raise RuntimeError(message)

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def finger(rows):
    content=json.dumps(sorted(rows,key=lambda r:json.dumps(r,sort_keys=True,ensure_ascii=False)),ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return hashlib.sha256(content.encode('utf8')).hexdigest()

def df_for(spark,rows):
    schema=T.StructType([T.StructField(k,T.StringType(),True) for k in FIELDS])
    return spark.createDataFrame([[str(r[k]) if r.get(k) is not None else None for k in FIELDS] for r in rows],schema)

def load_rows(spark,location):
    return [x.asDict() for x in spark.read.parquet(location).select(*FIELDS).collect()]

def publish(spark,fs,P,path,rows):
    require(rows,'Intento de publicar una tabla sin registros')
    expected=finger(rows)
    p=P(path)
    if fs.exists(p):
        require(fs.exists(P(path+'/_SUCCESS')),'Snapshot incompleto: '+path)
        require(finger(load_rows(spark,path))==expected,'Conflicto con snapshot existente: '+path)
        return 'EXISTENTE_VERIFICADO'
    stage=path+'_staging'
    require(not fs.exists(P(stage)),'Staging previo: '+stage+'; inspeccionar antes de reintentar')
    df_for(spark,rows).coalesce(1).write.mode('errorifexists').option('compression','snappy').parquet(stage)
    require(finger(load_rows(spark,stage))==expected,'Staging Parquet distinto: '+path)
    require(fs.rename(P(stage),p),'No fue posible renombrar staging: '+path)
    require(fs.exists(P(path+'/_SUCCESS')) and finger(load_rows(spark,path))==expected,
            'Verificacion del resultado fallo: '+path)
    return 'PUBLICADO_Y_VERIFICADO'

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--mode',required=True,choices=['ingest','approve'])
    ap.add_argument('--pdf',required=True)
    ap.add_argument('--manifest',required=True)
    args=ap.parse_args()
    pdf=Path(args.pdf); meta=json.loads(Path(args.manifest).read_text(encoding='utf8'))
    h=meta['sha256'];require(len(h)==64 and all(c in '0123456789abcdef' for c in h),'SHA invalido')
    require(pdf.is_file() and sha(pdf)==h,'No coincide PDF con SHA del manifiesto')
    spark=(SparkSession.builder.appName('OSIPTEL_Ingesta_PDF_Controlada_'+args.mode)
           .master('local[2]').config('spark.hadoop.fs.defaultFS',HDFS)
           .config('spark.sql.shuffle.partitions','2').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        P=spark.sparkContext._jvm.org.apache.hadoop.fs.Path
        fs=P(RAW).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        for root in (RAW,SILVER,GOLD):require(fs.exists(P(root)),'Falta directorio HDFS: '+root)
        original=str(meta['archivo_original'])[:180]
        family=meta['familia'];require(family in ('F2','F3','F4'),'Familia no permitida')
        raw_dir=f'{RAW}/{h}'
        raw_target=f'{raw_dir}/documento.pdf'
        raw_status='EXISTENTE_VERIFICADO'
        if args.mode=='ingest':
            if not fs.exists(P(raw_dir)):
                require(fs.mkdirs(P(raw_dir)),'No fue posible crear carpeta RAW')
            if not fs.exists(P(raw_target)):
                temp=f'{raw_dir}/_staging.pdf'
                require(not fs.exists(P(temp)),'RAW temporal previo pendiente de revisar')
                fs.copyFromLocalFile(False,False,P(pdf.as_uri()),P(temp))
                require(fs.rename(P(temp),P(raw_target)),'No fue posible publicar PDF RAW')
                raw_status='PUBLICADO_Y_VERIFICADO'
        require(fs.exists(P(raw_target)),'PDF RAW no encontrado; primero realizar ingesta Bronze/Silver')
        # Comprobacion de SHA-256 real del contenido en HDFS, no solo existencia.
        binary=spark.read.format('binaryFile').load(raw_target).select(F.sha2('content',256).alias('sha')).collect()
        require(len(binary)==1 and binary[0]['sha']==h,'Contenido RAW HDFS no coincide con SHA-256')
        candidates=meta.get('candidatos',[])
        silver_rows=[]
        for x in (candidates or [{}]):
            silver_rows.append({'pdf_sha256':h,'archivo_original':original,'fuente':family,
             'clasificacion':meta['clasificacion'],'pagina':str(x.get('pagina') or ''),
             'tipo':str(x.get('tipo') or 'CANDIDATO_SIN_TIPO'), 'accion':str(x.get('accion_detectada') or ''),
             'fragmento':str(x.get('fragmento') or '')[:2000], 'estado':'CANDIDATO_NO_APROBADO',
             'valor':None,'unidad':None,'periodo':None,'ambito':None,'revisor':None,
             'justificacion':None,'evidencia_sha256':h})
        silver_path=f'{SILVER}/{h}/v1'
        require(fs.exists(P(SILVER+'/'+h)) or fs.mkdirs(P(SILVER+'/'+h)),'No se pudo crear raiz Silver SHA')
        silver_status=publish(spark,fs,P,silver_path,silver_rows) if args.mode=='ingest' else 'EXISTENTE'
        require(fs.exists(P(silver_path+'/_SUCCESS')),'Silver no disponible; no se puede aprobar')
        if args.mode=='ingest':
            report={'estado':'BRONZE_SILVER_PUBLICADOS','sha256':h,'raw_pdf':raw_target,
                    'silver':silver_path,'raw_estado':raw_status,'silver_estado':silver_status,
                    'candidatos':len(candidates),'gold':'NO_PUBLICADO_REQUIERE_REVISION'}
        else:
            review=meta['revision']
            require(review['revisor'] and review['justificacion'],'Revision incompleta')
            gold_row={'pdf_sha256':h,'archivo_original':original,'fuente':family,
              'clasificacion':meta['clasificacion'],'pagina':str(review['pagina']),
              'tipo':str(review['tipo']), 'accion':str(review.get('accion') or ''),
              'fragmento':str(review['fragmento'])[:2000],
              'estado':'REVISION_HUMANA_REGISTRADA_NO_CERTIFICACION_INSTITUCIONAL',
              'valor':review.get('valor'),'unidad':review.get('unidad'),
              'periodo':review.get('periodo'),'ambito':review.get('ambito'),
              'revisor':review['revisor'],'justificacion':review['justificacion'],
              'evidencia_sha256':h}
            reviewhash=finger([gold_row])[:20]
            folder=f'{GOLD}/{h}'
            require(fs.exists(P(folder)) or fs.mkdirs(P(folder)),'No se pudo crear carpeta Gold SHA')
            existing=[str(item.getPath().getName()) for item in fs.listStatus(P(folder))
                      if item.isDirectory() and str(item.getPath().getName()).startswith('v1_')]
            dest=f'{folder}/v1_{reviewhash}'
            require(not existing or existing==['v1_'+reviewhash],
                    'Ya existe otro dictamen Gold para este PDF. Requiere conciliacion manual.')
            status=publish(spark,fs,P,dest,[gold_row])
            report={'estado':'GOLD_DOCUMENTAL_CON_REVISION_REGISTRADA','sha256':h,
                    'raw_pdf':raw_target,'silver':silver_path,'gold':dest,
                    'gold_estado':status,'nota':'No se integra automaticamente a KPI oficiales ni certifica validez legal'}
        print('RESULTADO_INGESTA='+json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
