#!/usr/bin/env python3
"""Solo lectura de snapshots HDFS validados; emite JSON de decisiones de alcance limitado."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

HDFS='hdfs://namenode:8020'
VERSION='v1_17b651ac372a96fb18fa'
GOLD_VERSION='v2_b6e4155ae58e8f9106d8'
BASE=f'{HDFS}/data/mart_documental_curado'
GOLD=f'{HDFS}/data/gold'
OUT=Path('/tmp/osiptel_decisiones.json')
TABLES={
  'F2':'f2_menciones_documentales_verificadas',
  'F3':'f3_cifras_contextualizadas_verificadas',
  'F4_verificados':'f4_hogares_internet_narrativa_verificados',
  'F4_pendientes':'f4_series_pendientes_revision',
  'F4_conflictos':'f4_conflictos_excluidos',
  'F1_F4':'comparacion_nacional_f1_f4_verificada',
}
EXPECTED={'F2':287,'F3':12,'F4_verificados':20,'F4_pendientes':74,'F4_conflictos':102,'F1_F4':5}

def require(c, msg):
    if not c: raise RuntimeError('PRUEBA_FALLIDA: '+msg)

def r(x,decimals=2):return round(float(x),decimals)

def main():
  spark=(SparkSession.builder.appName('OSIPTEL_Prueba_Integral_Decisiones')
      .master('local[2]').config('spark.hadoop.fs.defaultFS',HDFS)
      .config('spark.sql.shuffle.partitions','4').getOrCreate())
  spark.sparkContext.setLogLevel('ERROR')
  try:
    P=spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    fs=P(BASE).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    frames={}
    counts={}
    for k,table in TABLES.items():
      path=f'{BASE}/{table}/{VERSION}'
      require(fs.exists(P(path+'/_SUCCESS')),'No existe _SUCCESS en '+path)
      frames[k]=spark.read.parquet(path)
      counts[k]=frames[k].count()
      require(counts[k]==EXPECTED[k],f'{table}: filas {counts[k]}, esperadas {EXPECTED[k]}')
    fn=f'{GOLD}/osiptel_historico_nacional_f1/{GOLD_VERSION}'
    ft=f'{GOLD}/osiptel_historico_territorial_f1/{GOLD_VERSION}'
    for path in [fn,ft]: require(fs.exists(P(path+'/_SUCCESS')),'No existe Gold F1: '+path)
    nacional=spark.read.parquet(fn)
    territorial=spark.read.parquet(ft)
    require(nacional.count()==71,'Gold nacional F1 no tiene 71 periodos')
    require(territorial.count()==129225,'Gold territorial F1 no tiene 129225 registros')
    require(frames['F4_verificados'].select('periodo','ambito').distinct().count()==20,
            'Duplicados en clave trimestre/ambito F4')
    require(frames['F4_verificados'].filter((F.col('porcentaje').cast('double') < 0) | (F.col('porcentaje').cast('double')>100)).count()==0,
            'Porcentaje F4 fuera de rango')
    f4rows=frames['F4_verificados'].select('periodo','ambito','porcentaje','pdf','pagina_pdf','sha256_pdf').collect()
    serie={}
    fuentes={}
    for x in f4rows:
      serie.setdefault(x.periodo,{})[x.ambito]=r(x.porcentaje,1)
      fuentes.setdefault(x.periodo,{'pdf':x.pdf,'pagina':int(x.pagina_pdf),'sha256':x.sha256_pdf})
    require(len(serie)==5 and all(len(x)==4 for x in serie.values()),'Esperados cinco trimestres, cuatro ambitos')
    latest_f4=max(serie)
    last=serie[latest_f4]
    require('Area rural' in last and 'Lima Metropolitana' in last and 'Nacional' in last,
            'Faltan ambitos en INEI ultimo trimestre')
    gap=r(last['Lima Metropolitana']-last['Area rural'],1)
    # Serie nacional F1: SOLO stock de conexiones por mes, no cantidad de hogares.
    f1rows=[{'periodo':x.periodo,'conexiones':int(x.conexiones_fijas)} for x in
            nacional.select('periodo','conexiones_fijas').orderBy('periodo').collect()]
    f1by={x['periodo']:x['conexiones'] for x in f1rows}
    cierres=sorted([p for p in f1by if p.endswith(('-03','-06','-09','-12'))])
    require(len(cierres)>=2,'Sin dos cierres trimestrales Gold F1')
    previo,actual=cierres[-2:]
    a,b=f1by[previo],f1by[actual]
    require(a>0,'Valor inicial F1 invalido')
    dif=b-a
    difpct=r((b-a)*100/a,2)
    tr=territorial.filter(F.col('periodo').isin(previo,actual))
    tr=(tr.filter(F.col('departamento').isNotNull() & (F.length(F.trim('departamento'))>0))
      .withColumn('depto',F.upper(F.trim('departamento')))
      .groupBy('depto','periodo').agg(F.sum(F.col('conexiones_fijas').cast('long')).alias('total')))
    trows=tr.collect()
    bydept={}
    for x in trows: bydept.setdefault(x.depto,{})[x.periodo]=int(x.total)
    regions=[]
    for dept,v in bydept.items():
      if previo not in v or actual not in v: continue
      last_val=v[actual]; old_val=v[previo]
      pct=r(100*(last_val-old_val)/old_val,2) if old_val else None
      regions.append({'departamento':dept,'anterior':old_val,'actual':last_val,
                      'diferencia':last_val-old_val,'variacion_pct':pct})
    # Base pequeña crea cambios porcentuales volatiles; priorizar al menos 1000 conexiones anteriores.
    declines=sorted([x for x in regions if x['anterior']>=1000 and x['diferencia']<0],
                    key=lambda x:(x['variacion_pct'],x['diferencia']))
    allregions=sorted(regions,key=lambda x:x['departamento'])
    f2types=[{'tipo':x.tipo_hecho,'menciones':int(x.n)} for x in
             frames['F2'].groupBy('tipo_hecho').agg(F.count('*').alias('n')).orderBy(F.desc('n')).collect()]
    f3rows=[{'periodo':x.periodo_validado,'indicador':x.indicador_corregido,'valor':r(x.valor,2),
             'entidad':x.ambito_o_entidad,'naturaleza':x.naturaleza,
             'documento':x.documento_pdf,'pagina':int(x.pagina_pdf)} for x in
             frames['F3'].select('periodo_validado','indicador_corregido','valor','ambito_o_entidad','naturaleza','documento_pdf','pagina_pdf').collect()]
    # Comparacion F1-F4 deliberadamente descriptiva. No se restan unidades incompatibles.
    comparacion=[]
    for x in frames['F1_F4'].select('periodo_trimestre','conexiones_fijas_f1','hogares_internet_pct_f4','estado_cobertura','pdf_inei','pagina_pdf_inei').collect():
      comparacion.append({'periodo':x.periodo_trimestre,'conexiones_fijas':int(x.conexiones_fijas_f1) if x.conexiones_fijas_f1 is not None else None,
        'hogares_pct':r(x.hogares_internet_pct_f4,1), 'estado':x.estado_cobertura,
        'pdf_inei':x.pdf_inei,'pagina_pdf_inei':int(x.pagina_pdf_inei)})
    comparacion.sort(key=lambda x:x['periodo'])
    require(sum(x['estado']=='AMBAS_FUENTES' for x in comparacion)==4,
            'Se esperaban cuatro periodos con comparacion F1/F4')
    decisions=[{
      'codigo':'D01','prioridad':'ALTA','titulo':'Priorizar diagnostico de acceso a internet en zonas rurales',
      'indicador':f'INEI {latest_f4}: rural {last["Area rural"]:.1f}% frente a Lima Metropolitana {last["Lima Metropolitana"]:.1f}% ({gap:.1f} pp de diferencia)',
      'accion':'Solicitar estudio territorial rural y contrastar con otros datos antes de proponer despliegue.',
      'evidencia':fuentes[latest_f4]['pdf']+f', p. {fuentes[latest_f4]["pagina"]}',
      'limite':'La diferencia es entre ambitos, no estima hogares desconectados por departamento ni demuestra causa.'},
      {
      'codigo':'D02','prioridad':'ALTA' if dif<0 else 'MEDIA','titulo':'Investigar variaciones recientes de conexiones fijas',
      'indicador':f'OSIPTEL: {a:,} ({previo}) -> {b:,} ({actual}); variacion {difpct:+.2f}% ({dif:+,} conexiones)',
      'accion':'Revisar consistencia de cierre de reporte, bajas/altas de operadores, tecnologia y tramos departamentales.',
      'evidencia':'Gold F1 nacional y territorial, version '+GOLD_VERSION,
      'limite':'Variacion en stock de conexiones; no demuestra perdida de cobertura geografica ni numero de hogares.'},
      {
      'codigo':'D03','prioridad':'ALTA','titulo':'No incorporar conflictos INEI a indicadores finales',
      'indicador':'102 grupos conflictivos; 74 series aun sin verificacion semantica; 20 indicadores corroborados.',
      'accion':'Mantener 102 conflictos y 74 series pendientes fuera del set de indicadores verificados; programar curaduria.',
      'evidencia':'Mart documental curado F4 '+VERSION,
      'limite':'Los registros excluidos conservan valor exploratorio, no son cero ni datos faltantes asumidos.'},
      {
      'codigo':'D04','prioridad':'MEDIA','titulo':'Revisar regulacion y comportamiento de operadores como contexto',
      'indicador':'287 menciones regulatorias F2 y 12 cifras contextualizadas F3.',
      'accion':'Consultar expedientes y verificar estado regulatorio ante una decision de infraestructura o comparticion.',
      'evidencia':'F2 y F3 documental, '+VERSION,
      'limite':'Menciones no equivalen a mandatos aprobados; variaciones de operadores no prueban causalidad.'}
    ]
    if declines:
       d=declines[0]
       decisions[1]['accion'] += f' Comenzar revision por {d["departamento"]}: {d["variacion_pct"]:+.2f}% y {d["diferencia"]:+,} conexiones (base >=1000).'
    report={
      'estado':'GENERADO_DESDE_HDFS','generado_utc':datetime.now(timezone.utc).isoformat(),
      'version_curada':VERSION,'version_gold_f1':GOLD_VERSION,
      'controles':{'tablas_curadas':counts,'F1_nacional':71,'F1_territorial':129225,
                   'periodos_inei':5,'ambitos_inei':4,'comparaciones_temporales_con_F1':4},
      'inei':{'periodos':sorted(serie),'series':dict(sorted(serie.items())),
              'fuentes':fuentes,'ultimo_periodo':latest_f4,'brecha_rural_lima_pp':gap},
      'osiptel_f1':{'periodo_anterior':previo,'periodo_actual':actual,
                   'conexiones_anterior':a,'conexiones_actual':b,'variacion_absoluta':dif,
                   'variacion_pct':difpct,'departamentos_analizados':len(regions),
                   'departamentos_con_caida_y_base_minima':len(declines),
                   'prioridad_revision_departamental':declines[:10],
                   'detalle_departamental':allregions},
      'regulacion_f2':f2types,'estadisticas_f3':f3rows,'comparacion':comparacion,
      'decisiones':decisions,
      'advertencia':'Priorizacion exploratoria para investigacion, no autorizacion de inversion ni mandato. F1 = conexiones fijas; F4 = porcentaje de hogares con cualquier internet.'
    }
    OUT.parent.mkdir(parents=True,exist_ok=True)
    tmp=OUT.with_name(OUT.name+'.tmp')
    tmp.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    os.replace(tmp,OUT)
    print('DECISIONES_OK',json.dumps({'path':str(OUT),'f1_ultimo_periodo':actual,'f4_ultimo_periodo':latest_f4,
         'brecha_pp':gap,'territorios':len(regions),'priorizados':len(declines),'decisiones':len(decisions)},ensure_ascii=False),flush=True)
  finally:
    spark.stop()

if __name__=='__main__':main()
