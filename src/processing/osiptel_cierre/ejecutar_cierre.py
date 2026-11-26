#!/usr/bin/env python3
"""Orquestacion unica de auditoria local + Spark + publicacion inmutable HDFS."""
import argparse
import fcntl
import json
import os
import selectors
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE=Path(__file__).resolve().parent
STATUS_DIR=Path('/srv/osiptel/evidence/osiptel_cierre_curado')
SPARK_SCRIPT='/workspace/scripts/osiptel_cierre/publicar_curados_spark.py'


def utc():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def save_json(path, data):
    tmp=path.with_name(path.name+'.tmp')
    tmp.write_text(json.dumps(data,indent=2,ensure_ascii=False)+'\n',encoding='utf8')
    os.replace(str(tmp),str(path))


def stream(cmd, log, seconds):
    process=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,bufsize=0)
    select=selectors.DefaultSelector()
    select.register(process.stdout,selectors.EVENT_READ)
    started=time.monotonic()
    while True:
        if time.monotonic()-started>seconds:
            process.terminate()
            try: process.wait(timeout=8)
            except subprocess.TimeoutExpired: process.kill()
            raise RuntimeError('Tiempo maximo alcanzado; revisar job Spark antes de reintentar')
        events=select.select(timeout=0.5)
        for key,_ in events:
            chunk=os.read(key.fileobj.fileno(),65536)
            if chunk:
                log.write(chunk)
                log.flush()
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            else:
                select.unregister(key.fileobj)
        if process.poll() is not None and not select.get_map():
            break
    return process.wait()


def main():
    ap=argparse.ArgumentParser()
    g=ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--preflight',action='store_true')
    g.add_argument('--publish',action='store_true')
    ap.add_argument('--timeout',type=int,default=300)
    args=ap.parse_args()
    mode='publish' if args.publish else 'preflight'
    STATUS_DIR.mkdir(parents=True,exist_ok=True)
    with (STATUS_DIR/'cierre.lock').open('a') as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print('YA_HAY_UN_JOB_EN_EJECUCION',flush=True)
            return 2
        timestamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        logpath=STATUS_DIR/('ejecucion_'+timestamp+'_'+mode+'.log')
        statuspath=STATUS_DIR/'estado_actual.json'
        status={'fase':'EJECUTANDO','modo':mode,'inicio_utc':utc(),'fin_utc':None,'log':str(logpath),'exit_code':None,
                'nota':'F2 menciones, F3 cifras, F4 subconjunto verificado. Otros F4 se mantienen excluidos.'}
        save_json(statuspath,status)
        try:
            with logpath.open('wb') as log:
                print('=== VALIDACION ARCHIVOS ===',flush=True)
                rc=stream([sys.executable,str(HERE/'validar_paquete.py')],log,30)
                if rc: raise RuntimeError('Falló la integridad de archivos revisados')
                if args.publish:
                    print('=== PREPARANDO HDFS CURADO ===',flush=True)
                    rc=stream(['bash',str(HERE/'preparar_hdfs.sh')],log,360)
                    if rc: raise RuntimeError('No se pudo preparar destino HDFS')
                sparkcmd=['docker','exec','osiptel-bigdata-spark-1','timeout','-s','TERM','-k','15s',str(args.timeout)+'s',
                          '/opt/spark/bin/spark-submit',SPARK_SCRIPT,
                          '--publish' if args.publish else '--preflight']
                print('=== INICIANDO SPARK ===',flush=True)
                rc=stream(sparkcmd,log,args.timeout+40)
                if rc: raise RuntimeError('Spark terminó con código '+str(rc))
            # El Spark publica el resumen al final; copiamos un estado resumido apto para UI.
            full=logpath.read_text(encoding='utf8',errors='replace')
            marker='=== RESUMEN CIERRE CURADO OSIPTEL ==='
            if marker in full:
                tail=full.split(marker)[-1].lstrip()
                parsed=json.JSONDecoder().raw_decode(tail)[0]
                save_json(STATUS_DIR/'ultimo_resumen.json',parsed)
            status.update(fase='COMPLETADO',exit_code=0)
        except Exception as e:
            status.update(fase='ERROR',exit_code=1,error=str(e))
            print('ERROR_CIERRE:',e,flush=True)
        status['fin_utc']=utc()
        save_json(statuspath,status)
        print('ESTADO_FINAL:',status['fase'],'| codigo:',status['exit_code'],flush=True)
        return status['exit_code']

if __name__=='__main__':
    sys.exit(main())
