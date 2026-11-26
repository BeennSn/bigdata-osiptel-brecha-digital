"""Rutas adicionales para el panel 8767. Nunca aprueba automáticamente documentos nuevos."""
import json
import re
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import parse_qs

from .controlar_pdf import RUN, STATE, VALID_NAME, safe_pdf, eval_doc, write_json, HERE


def respond(h, status, obj):
    raw=json.dumps(obj,ensure_ascii=False).encode('utf8')
    h.send_response(status)
    h.send_header('Content-Type','application/json; charset=utf-8')
    h.send_header('Cache-Control','no-store')
    h.send_header('X-Content-Type-Options','nosniff')
    h.send_header('Content-Length',str(len(raw)))
    h.end_headers(); h.wfile.write(raw)


def read_state(name):
    p=STATE/(name+'.json')
    if p.is_file():
        try:return json.loads(p.read_text(encoding='utf8'))
        except (OSError,ValueError):pass
    return {'estado':'AUN_NO_INGESTADO','archivo':name}


def route_get(h,u):
    if u.path!='/api/nuevo/estado':return False
    name=parse_qs(u.query).get('file',[''])[0]
    if not VALID_NAME.fullmatch(name):respond(h,400,{'error':'Identificador incorrecto'});return True
    respond(h,200,{'proceso':read_state(name)})
    return True


def route_post(h,u,query):
    if u.path not in ('/api/nuevo/ingestar','/api/nuevo/aprobar'):return False
    name=query.get('file',[''])[0]
    if not VALID_NAME.fullmatch(name):respond(h,400,{'error':'Identificador incorrecto'});return True
    try:
        safe_pdf(name)
        doc=eval_doc(safe_pdf(name))
        old=read_state(name)
        mode='ingest' if u.path.endswith('ingestar') else 'approve'
        if old.get('estado') in ('EJECUTANDO','SOLICITADO'):
            respond(h,409,{'error':'Ya hay una accion activa para este documento'});return True
        if mode=='approve' and old.get('estado') not in ('BRONZE_SILVER_PUBLICADOS','GOLD_DOCUMENTAL_CON_REVISION_REGISTRADA'):
            respond(h,409,{'error':'Primero ejecutar y comprobar Bronze + Silver'});return True
        STATE.mkdir(parents=True,exist_ok=True)
        args=[sys.executable,str(HERE/'controlar_pdf.py'),'--mode',mode,'--file',name]
        if mode=='approve':
            clen=int(h.headers.get('Content-Length','-1'))
            if not 2<=clen<=5000:respond(h,400,{'error':'Formulario de revision excede limite'});return True
            body=h.rfile.read(clen)
            payload=json.loads(body)
            if not isinstance(payload,dict):raise ValueError('Formato de revision invalido')
            # No se admite un dictamen vacío ni valores arbitrarios gigantes.
            allowed={'revisor','justificacion','candidato_index','valor','unidad','periodo','ambito'}
            payload={k:v for k,v in payload.items() if k in allowed}
            reviewfile=STATE/('revision_'+uuid.uuid4().hex+'.json')
            write_json(reviewfile,payload)
            reviewfile.chmod(0o600)
            args+=['--review',str(reviewfile)]
        write_json(STATE/(name+'.json'),{'estado':'SOLICITADO','modo':mode,'archivo':name})
        with (STATE/'lanzamientos.log').open('ab') as log:
            proc=subprocess.Popen(args,cwd=str(HERE),stdout=log,stderr=subprocess.STDOUT,
                                 start_new_session=True,close_fds=True)
        respond(h,202,{'mensaje':'Ejecucion solicitada: '+mode,'pid':proc.pid,'archivo':name})
    except (ValueError, OSError, KeyError, json.JSONDecodeError) as e:
        respond(h,422,{'error':str(e)[:400]})
    return True
