#!/usr/bin/env python3
"""Prueba viva contra OCI: hashes, re-ejecucion, idempotencia HDFS, interfaz y decision."""
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

BASE=Path('/srv/osiptel/scripts/osiptel_cierre')
HERE=Path(__file__).resolve().parent
OUT=HERE/'salida'
VERSION='v1_17b651ac372a96fb18fa'
TABLES=['f2_menciones_documentales_verificadas','f3_cifras_contextualizadas_verificadas',
        'f4_hogares_internet_narrativa_verificados','f4_series_pendientes_revision',
        'f4_conflictos_excluidos','comparacion_nacional_f1_f4_verificada']
REPORT=OUT/'reporte_pruebas.json'

def stage(label, fn):
    print('\n=== '+label+' ===',flush=True)
    tick=time.monotonic()
    try:
        value=fn()
        RESULT['pruebas'].append({'prueba':label,'estado':'OK','segundos':round(time.monotonic()-tick,1),'detalle':value})
        print('PRUEBA_OK '+label+': '+str(value)[:700],flush=True)
    except Exception as e:
        RESULT['pruebas'].append({'prueba':label,'estado':'ERROR','segundos':round(time.monotonic()-tick,1),'error':str(e)})
        print('PRUEBA_ERROR '+label+': '+str(e),flush=True)
        raise

def run(name,args,timeout):
    p=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding='utf-8',errors='replace',timeout=timeout)
    log=OUT/(name+'.log'); log.write_text(p.stdout,encoding='utf-8')
    if p.returncode!=0:
        raise RuntimeError(f'{name}: codigo {p.returncode}. Ultimas lineas: '+ '\n'.join(p.stdout.splitlines()[-18:]))
    return p.stdout

def hashes():
    run('ajuste_preflight',[sys.executable,str(HERE/'reparar_preflight.py')],10)
    assert BASE.exists(),f'No existe {BASE}. Ejecute dentro de Oracle, no en WSL.'
    return '\n'.join(run('validacion_archivos',[sys.executable,str(BASE/'validar_paquete.py')],45).splitlines()[-2:])

def hdfs_success():
    script="""set -eu
v=v1_17b651ac372a96fb18fa
for t in f2_menciones_documentales_verificadas f3_cifras_contextualizadas_verificadas f4_hogares_internet_narrativa_verificados f4_series_pendientes_revision f4_conflictos_excluidos comparacion_nacional_f1_f4_verificada; do
hdfs dfs -test -e \"/data/mart_documental_curado/$t/$v/_SUCCESS\" || exit 7
echo \"OK $t\"
done"""
    output=run('hdfs_success',['docker','exec','osiptel-bigdata-namenode-1','sh','-lc',script],120)
    lines=[x for x in output.splitlines() if x.startswith('OK ')]
    assert len(lines)==6,'Se esperaban 6 _SUCCESS, encontrados '+str(len(lines))
    return lines

def preflight():
    out=run('pipeline_preflight',[sys.executable,str(BASE/'ejecutar_cierre.py'),'--preflight','--timeout','450'],600)
    assert 'CONTROLES_OK' in out and 'ESTADO_FINAL: COMPLETADO | codigo: 0' in out,'Preflight sin controles OK o no completo'
    return 'Spark: CONTROLES_OK; sin publicacion'

def idempotencia():
    out=run('pipeline_repetido',[sys.executable,str(BASE/'ejecutar_cierre.py'),'--publish','--timeout','450'],1100)
    assert 'ESTADO_FINAL: COMPLETADO | codigo: 0' in out,'Falló segunda publicación'
    lines=[x for x in out.splitlines() if '[CIERRE_OSIPTEL] OK ' in x and 'EXISTENTE_VERIFICADO' in x]
    assert len(lines)==6,f'No se verificaron 6 snapshots existentes: {len(lines)}. ' + '\n'.join(out.splitlines()[-15:])
    summary=json.loads(Path('/srv/osiptel/evidence/osiptel_cierre_curado/ultimo_resumen.json').read_text())
    assert summary['version']==VERSION,'Versión inesperada'
    assert len(summary.get('publicaciones',[]))==6,'Resumen incompleto'
    assert all(x['estado']=='EXISTENTE_VERIFICADO' for x in summary['publicaciones']),'No fue idempotente'
    return 'Seis tablas existentes verificadas por firma sin crear una versión nueva'

def decisiones():
    out=run('generar_decisiones_spark',['docker','exec','osiptel-bigdata-spark-1','timeout','-s','TERM','-k','15s','450s',
            '/opt/spark/bin/spark-submit','/workspace/scripts/osiptel_pruebas/generar_decisiones_spark.py'],510)
    temporal=OUT/'decisiones.json.copia'
    run('copiar_decisiones',['docker','cp','osiptel-bigdata-spark-1:/tmp/osiptel_decisiones.json',str(temporal)],60)
    import os
    os.replace(temporal,OUT/'decisiones.json')
    p=OUT/'decisiones.json'; assert p.is_file(),'No se recuperó decisiones.json del contenedor'
    d=json.loads(p.read_text(encoding='utf-8'))
    assert d['estado']=='GENERADO_DESDE_HDFS' and len(d['decisiones'])==4,'Reporte de decisiones incompleto'
    assert d['controles']['tablas_curadas']=={'F2':287,'F3':12,'F4_verificados':20,'F4_pendientes':74,'F4_conflictos':102,'F1_F4':5}
    assert 'DECISIONES_OK' in out,'Spark sin confirmación decisiones OK'
    return {'ultimo_periodo_inei':d['inei']['ultimo_periodo'],
            'ultimo_periodo_f1':d['osiptel_f1']['periodo_actual'],
            'diferencia_lima_rural_pp':d['inei']['brecha_rural_lima_pp'],
            'departamentos_evaluados':d['osiptel_f1']['departamentos_analizados'],
            'departamentos_priorizados':d['osiptel_f1']['departamentos_con_caida_y_base_minima']}

def request(base,path,method='GET',data=None,headers=None,timeout=8):
    try:
      with urlopen(Request(base+path,method=method,data=data,headers=headers or {}),timeout=timeout) as r:
        return r.status,r.read().decode('utf-8')
    except HTTPError as e:
      return e.code,e.read().decode('utf-8')

def smoke_ui():
    # Servidor de prueba en loopback, no modifica ni detiene el panel real.
    with socket.socket() as s:
      s.bind(('127.0.0.1',0)); port=s.getsockname()[1]
    env=os.environ.copy(); env['OSIPTEL_UI_PORT']=str(port)
    proc=subprocess.Popen([sys.executable,str(BASE/'interfaz_osiptel.py')],cwd=str(BASE),env=env,
                          stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    addr=f'http://127.0.0.1:{port}'
    try:
      for _ in range(30):
        if proc.poll() is not None: raise RuntimeError('Interfaz terminó antes de abrir HTTP')
        try:
          status,body=request(addr,'/',timeout=1)
          if status==200:break
        except (URLError,TimeoutError):time.sleep(0.2)
      else: raise RuntimeError('Interfaz no responde en puerto de prueba')
      assert 'OSIPTEL' in body and 'const token=' in body,'HTML incorrecto'
      match=re.search(r'const token=("[^"]+");',body)
      assert match,'Token CSRF no presente'
      token=json.loads(match.group(1))
      for route in ['/api/status','/api/summary','/api/uploads']:
        status,text=request(addr,route)
        assert status==200 and isinstance(json.loads(text),dict),f'Error GET {route}'
      code,_=request(addr,'/api/start?mode=preflight',method='POST',data=b'',headers={'X-CSRF':'token_invalido'})
      assert code==403,'No rechazó solicitud sin CSRF valido'
      name='PRUEBA_NO_INGESTAR_'+uuid.uuid4().hex[:10]+'.csv'
      data=b'PRUEBA,ESTADO\nSMOKE_TEST,CUARENTENA_NO_PROCESAR\n'
      code,resp=request(addr,'/api/upload?filename='+name,method='POST',data=data,
               headers={'X-CSRF':token,'Content-Type':'application/octet-stream'})
      assert code==201 and 'CUARENTENA' in resp, 'La carga a cuarentena falló: '+resp
      info=json.loads(resp); dest=Path(os.getenv('OSIPTEL_RUN_DIR','/srv/osiptel/evidence/osiptel_cierre_curado'))/'cuarentena_upload'/info['archivo']
      assert dest.is_file() and dest.read_bytes()==data,'La carga no conservó contenido'
      dest.unlink() # elimina exclusivamente el archivo sintético de esta prueba
      return 'GET web/APIs OK, CSRF 403 OK y carga temporal en cuarentena OK (archivo de prueba retirado)'
    finally:
      proc.terminate()
      try: proc.wait(timeout=5)
      except subprocess.TimeoutExpired: proc.kill();proc.wait()

RESULT={'inicio_utc':datetime.now(timezone.utc).isoformat(),'estado':'EJECUTANDO','pruebas':[],
        'nota':'Las pruebas reusan snapshots ya auditados. No automatizan la aprobacion ni ingesta de nuevos PDF.'}

def save():
    OUT.mkdir(parents=True,exist_ok=True)
    tmp=REPORT.with_name(REPORT.name+'.tmp')
    tmp.write_text(json.dumps(RESULT,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    os.replace(tmp,REPORT)

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    save()
    tests=[('01 CSV y SHA-256',hashes),('02 HDFS _SUCCESS',hdfs_success),
           ('03 Spark preflight',preflight),('04 Publicacion idempotente',idempotencia),
           ('05 Indicadores y decisiones reales',decisiones),('06 Interfaz HTTP/CSRF/cuarentena',smoke_ui)]
    try:
       for name,fn in tests:
         stage(name,fn);save()
       RESULT['estado']='APROBADO_TECNICAMENTE'
    except Exception as e:
       RESULT['estado']='ERROR_PRUEBA';RESULT['error']=str(e)
    RESULT['fin_utc']=datetime.now(timezone.utc).isoformat();save()
    print('\n=== REPORTE FINAL ===',flush=True)
    print(json.dumps({'estado':RESULT['estado'],'pasadas':sum(x['estado']=='OK' for x in RESULT['pruebas']),
                     'total':len(tests),'reporte':str(REPORT)},ensure_ascii=False),flush=True)
    return 0 if RESULT['estado']=='APROBADO_TECNICAMENTE' else 1

if __name__=='__main__':sys.exit(main())
