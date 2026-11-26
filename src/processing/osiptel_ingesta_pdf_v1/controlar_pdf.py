#!/usr/bin/env python3
"""Orquesta una ingesta documental explícita sin modificar snapshots históricos.

La decisión de Gold la registra un revisor humano; no hay aprobación automática.
"""
import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE=Path(__file__).resolve().parent
PANEL=Path('/srv/osiptel/scripts/osiptel_pruebas')
RUN=Path(os.environ.get('OSIPTEL_RUN_DIR','/srv/osiptel/evidence/osiptel_cierre_curado'))
QUARANTINE=RUN/'cuarentena_upload'
EVALUATIONS=RUN/'evaluaciones_pdf'
STATE=RUN/'pdf_nuevos_controlados'
CONTAINER='osiptel-bigdata-spark-1'
VALID_NAME=re.compile(r'^[0-9a-f]{12}_[^/\\]{1,130}\.pdf$',re.I)


def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')
def require(condition,msg):
    if not condition:raise ValueError(msg)

def sha_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def write_json(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.tmp')
    tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    os.replace(tmp,path)


def safe_pdf(name):
    require(VALID_NAME.fullmatch(name) is not None,'Archivo no identificado como PDF de cuarentena')
    p=QUARANTINE/name
    require(p.is_file() and not p.is_symlink(),'PDF no encontrado en cuarentena')
    require(0<p.stat().st_size<=30*1024*1024,'PDF supera el limite de 30 MiB')
    with p.open('rb') as f:require(f.read(5)==b'%PDF-','Cabecera de PDF invalida')
    return p


def eval_doc(pdf):
    ep=EVALUATIONS/(pdf.name+'.json')
    require(ep.is_file(),'Antes de ingresar a HDFS, usa Subir y evaluar PDF')
    d=json.loads(ep.read_text(encoding='utf8'))
    require(d.get('sha256')==sha_file(pdf),'La evaluacion no corresponde al PDF actual')
    require(not d.get('registros_auditados_encontrados'),'Documento ya auditado: no duplicar datos')
    require(d.get('clasificacion_documental_v2') not in ('FUERA_DE_ALCANCE_ANALITICO',),
            'Fuera del alcance analitico; no se ingesta')
    fam=d.get('familias_candidatas') or []
    require(len(fam)==1 and fam[0] in ('F2','F3','F4'),
            'No se identifico fuente F2, F3 o F4; requiere clasificacion manual previa')
    require(d.get('paginas_extraidas')==d.get('paginas_totales'),
            'PDF parcialmente extraido; no ingresar sin completar la extraccion')
    return d


def validate_review(pdf,d,review):
    require(isinstance(review,dict),'Revision requerida')
    reviewer=str(review.get('revisor','')).strip()
    reason=str(review.get('justificacion','')).strip()
    require(3<=len(reviewer)<=100,'Indicar responsable real de la revision (3-100 caracteres)')
    require(25<=len(reason)<=1000,'Justificar con al menos 25 caracteres la revision documental')
    candidates=d.get('candidatos_nuevos') or []
    require(candidates,'No hay evidencias candidatas que aprobar')
    try:i=int(review.get('candidato_index'))
    except (TypeError,ValueError):raise ValueError('Seleccionar un indice de evidencia valido')
    require(0<=i<len(candidates),'Indice fuera de los candidatos extraidos')
    candidate=candidates[i]
    page=int(candidate.get('pagina') or 0)
    require(1<=page<=int(d['paginas_extraidas']),'Pagina de evidencia invalida')
    frag=str(candidate.get('fragmento') or '').strip()
    require(len(frag)>=12,'Fragmento sin evidencia suficiente')
    fam=d['familias_candidatas'][0]
    result={'revisor':reviewer,'justificacion':reason,'pagina':page,
      'tipo':str(candidate.get('tipo') or 'CANDIDATO_DOCUMENTAL'),
      'accion':str(candidate.get('accion_detectada') or ''),
      'fragmento':frag[:2000],'valor':None,'unidad':None,'periodo':None,'ambito':None}
    if fam in ('F3','F4'):
        value=str(review.get('valor','')).strip()
        unit=str(review.get('unidad','')).strip()
        period=str(review.get('periodo','')).strip()
        region=str(review.get('ambito','')).strip()
        require(bool(value and unit and period and region),'F3/F4 requieren valor, unidad, periodo y ambito')
        try:val=float(value.replace(',','.'))
        except ValueError:raise ValueError('Valor numerico invalido')
        require(abs(val)<1e12,'Valor fuera de limite tecnico')
        require(re.fullmatch(r'20\d{2}-(?:T[1-4]|(?:0[1-9]|1[0-2]))',period) is not None,
                'Periodo debe ser AAAA-Tn o AAAA-MM')
        require(2<=len(region)<=100 and 3<=len(unit)<=60,'Unidad o ambito incompletos')
        if fam=='F4':
            require(unit=='porcentaje_hogares','F4 exige unidad porcentaje_hogares')
            require(0<=val<=100,'Porcentaje F4 fuera de [0,100]')
            require(re.fullmatch(r'20\d{2}-T[1-4]',period) is not None,'F4 exige trimestre')
        # Cotejo numerico directo con pagina del PDF; no es validacion semantica automatica.
        sys.path.insert(0,str(PANEL))
        import evaluar_pdf_v1
        parts=evaluar_pdf_v1.inspect_pdf(pdf)[1]
        require(page<=len(parts) and evaluar_pdf_v1.has_number(parts[page-1],value),
                'La cifra indicada no aparece en la pagina original: no publicar Gold')
        result.update(valor=str(val),unidad=unit,periodo=period,ambito=region)
    return result


def docker(args, log, timeout=600):
    proc=subprocess.run(['docker']+args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                        text=True,errors='replace',timeout=timeout)
    log.write(proc.stdout+'\n')
    if proc.returncode:
        raise RuntimeError('Comando Docker fallo: '+str(args[:3])+' | '+proc.stdout[-1100:])
    return proc.stdout


def run(name,mode,revision_path=None, dry_run=False):
    pdf=safe_pdf(name)
    d=eval_doc(pdf)
    h=d['sha256']; STATE.mkdir(parents=True,exist_ok=True)
    lock_path=STATE/'pipeline.lock'
    with lock_path.open('a+') as f:
      fcntl.flock(f,fcntl.LOCK_EX)
      st=STATE/(name+'.json')
      logs=STATE/(name+'.log')
      result={'estado':'EJECUTANDO','modo':mode,'archivo':name,'sha256':h,'inicio_utc':now(),
              'nota':'Sin modificacion de Gold oficial F1 ni marts publicados'}
      write_json(st,result)
      try:
        review=None
        if mode=='approve':
            require(revision_path is not None and revision_path.is_file(),'Solicitud de revision no encontrada')
            review=validate_review(pdf,d,json.loads(revision_path.read_text(encoding='utf8')))
        manifest={'sha256':h,'archivo_original':d['nombre_archivo'],
                  'familia':d['familias_candidatas'][0],
                  'clasificacion':d.get('clasificacion_documental_v2') or d['estado'],
                  'candidatos':d.get('candidatos_nuevos') or []}
        if review is not None:manifest['revision']=review
        if dry_run:
            result.update(estado='PREVALIDACION_OK_SIN_HDFS',candidatos=len(manifest['candidatos']),
                          revision=review)
        else:
            temp=STATE/(h+'_'+mode+'.json')
            write_json(temp,manifest)
            target_pdf=f'/tmp/osiptel_{h}.pdf';target_json=f'/tmp/osiptel_{h}_{mode}.json'
            with logs.open('w',encoding='utf8') as logfile:
                docker(['cp',str(pdf),CONTAINER+':'+target_pdf],logfile,90)
                docker(['cp',str(temp),CONTAINER+':'+target_json],logfile,90)
                output=docker(['exec',CONTAINER,'timeout','-s','TERM','-k','15s','360s',
                       '/opt/spark/bin/spark-submit','/workspace/scripts/osiptel_ingesta_pdf_v1/publicar_pdf_spark.py',
                       '--mode',mode,'--pdf',target_pdf,'--manifest',target_json],logfile,420)
                outcomes=[line.split('=',1)[1] for line in output.splitlines() if line.startswith('RESULTADO_INGESTA=')]
                require(len(outcomes)==1,'Spark no emitio reporte de publicacion')
                result.update(json.loads(outcomes[0]))
                result['log']=str(logs)
            temp.unlink(missing_ok=True)
        result['fin_utc']=now()
        write_json(st,result)
        return result
      except Exception as e:
        result.update(estado='ERROR',error=str(e)[:2000],fin_utc=now(),log=str(logs))
        write_json(st,result)
        raise


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--file',required=True)
    ap.add_argument('--mode',choices=['ingest','approve'],required=True)
    ap.add_argument('--review')
    ap.add_argument('--dry-run',action='store_true')
    args=ap.parse_args()
    try:
        result=run(args.file,args.mode,Path(args.review) if args.review else None,args.dry_run)
        print('RESULTADO_CONTROLADO='+json.dumps(result,ensure_ascii=False))
    except Exception as e:
        print('ERROR_CONTROLADO='+str(e),file=sys.stderr)
        sys.exit(1)

if __name__=='__main__':main()
