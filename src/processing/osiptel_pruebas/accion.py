#!/usr/bin/env python3
"""Controlador de acciones serializadas; guarda estado JSON accesible al panel."""
import argparse
import fcntl
import json
import os
import subprocess
import sys
from datetime import datetime,timezone
from pathlib import Path

HERE=Path(__file__).resolve().parent
OUT=HERE/'salida'
BASE=Path('/srv/osiptel/scripts/osiptel_cierre')

def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')
def write(path,data):
  tmp=path.with_suffix(path.suffix+'.tmp')
  tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
  os.replace(tmp,path)

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument('--task',required=True,choices=['todo','decisiones','preflight','publish'])
  args=ap.parse_args()
  OUT.mkdir(parents=True,exist_ok=True)
  with (OUT/'accion.lock').open('a') as lock:
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
      print('ACCION_OCUPADA: existe una ejecucion activa',flush=True)
      return 2
    fix=subprocess.run([sys.executable,str(HERE/'reparar_preflight.py')],capture_output=True,text=True)
    if fix.returncode!=0:
      print('ERROR_AJUSTE_PREFLIGHT:',fix.stdout,fix.stderr,flush=True)
      return 1
    commands={
      'todo':[sys.executable,str(HERE/'probar_integracion.py')],
      'decisiones':[sys.executable,str(HERE/'solo_decisiones.py')],
      'preflight':[sys.executable,str(BASE/'ejecutar_cierre.py'),'--preflight','--timeout','450'],
      'publish':[sys.executable,str(BASE/'ejecutar_cierre.py'),'--publish','--timeout','450'],
    }
    state={'tarea':args.task,'fase':'EJECUTANDO','inicio':now(),'fin':None,'codigo':None,'log':str(OUT/'accion_ultima.log')}
    write(OUT/'estado_panel.json',state)
    try:
      with (OUT/'accion_ultima.log').open('wb') as f:
        result=subprocess.run(commands[args.task],stdout=f,stderr=subprocess.STDOUT,timeout=1300)
      state.update(fase='COMPLETADO' if result.returncode==0 else 'ERROR',codigo=result.returncode)
    except Exception as e:
      state.update(fase='ERROR',codigo=1,error=str(e))
    state['fin']=now()
    write(OUT/'estado_panel.json',state)
    print('ACCION_FINAL',state['fase'],'codigo',state['codigo'],flush=True)
    return int(state['codigo'])

if __name__=='__main__':sys.exit(main())
