#!/usr/bin/env python3
"""Evaluación documental no destructiva de PDF en cuarentena OSIPTEL.
Solo documentos corroborados por SHA pueden heredar una verificación anterior.
Archivos nuevos -> observación y candidatos; jamás pasan a Gold automáticamente.
"""
import csv
import hashlib
import re
import shutil
import subprocess
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

BASE=Path('/srv/osiptel/scripts/osiptel_cierre/inputs')
MAX_PDF_BYTES=30*1024*1024
MAX_PAGES=35
MAX_HALLAZGOS=30

def csv_rows(filename):
    p=BASE/filename
    if not p.exists():return []
    with p.open(encoding='utf-8-sig',newline='') as f:
        return list(csv.DictReader(f))

def hash_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()

def norm(s):
    s=unicodedata.normalize('NFKD',str(s))
    s=''.join(c for c in s if not unicodedata.combining(c))
    return ' '.join(s.casefold().split())

def inspect_pdf(path):
    """Extrae texto con poppler; sin OCR y con límites de CPU, tamaño y páginas."""
    if not shutil.which('pdfinfo') or not shutil.which('pdftotext'):
        raise RuntimeError('Faltan pdfinfo/pdftotext. En Oracle: sudo apt-get install -y poppler-utils')
    p=subprocess.run(['pdfinfo',str(path)],capture_output=True,text=True,timeout=15)
    if p.returncode:
        raise RuntimeError('No se pudo abrir PDF (posible PDF cifrado o corrupto): '+p.stderr.strip()[:180])
    m=re.search(r'^Pages:\s*(\d+)\s*$',p.stdout,re.M)
    if not m:raise RuntimeError('No se pudo determinar numero de paginas')
    total=int(m.group(1))
    if not 1<=total<=1000:raise RuntimeError('Cantidad de paginas fuera del limite de seguridad')
    n=min(total,MAX_PAGES)
    p=subprocess.run(['pdftotext','-f','1','-l',str(n),'-layout','-enc','UTF-8',str(path),'-'],capture_output=True,timeout=35)
    if p.returncode:
        raise RuntimeError('No fue posible extraer el texto del PDF')
    text=p.stdout.decode('utf-8','replace')
    pages=text.split('\f')
    if pages and not pages[-1].strip():pages.pop()
    return total,[x[:120000] for x in pages[:n]]

def has_number(text,val):
    """Busca valor tal como aparece, admitiendo coma/punto para decimales y separadores de miles."""
    v=str(val).strip()
    try:f=float(v)
    except (ValueError,TypeError):return False
    if abs(f-round(f))<1e-6:
        digits=str(int(round(f)))
        if len(digits)>=4:
            chunks=[];rest=digits
            while rest:
                chunks.append(rest[-3:]);rest=rest[:-3]
            groups=list(reversed(chunks))
            pattern=r'(?<!\d)' +r'[\s\.,]*'.join(map(re.escape,groups))+r'(?!\d)'
        else:pattern=r'(?<!\d)'+re.escape(digits)+r'(?!\d)'
        return bool(re.search(pattern,text))
    # formato latino habitual: 64,5; formato inglés: 64.5
    a,b=('{:.5f}'.format(f).rstrip('0').rstrip('.')).split('.')
    return bool(re.search(r'(?<!\d)'+re.escape(a)+r'[,.]'+re.escape(b)+r'(?!\d)',text))

def evidence_snippets(pages):
    triggers=('internet','fibra optica','banda ancha','conexiones','comparticion','mandato','cobertura','infraestructura')
    rows=[]
    for page,text in enumerate(pages,1):
        lines=[' '.join(s.split()) for s in text.splitlines()]
        for i,line in enumerate(lines):
            normalized=norm(line)
            if any(t in normalized for t in triggers) and re.search(r'\d[\d\s,.]*[%]?',line):
                if len(line)<14:continue
                rows.append({'pagina':page,'fragmento':line[:240],'estado':'CANDIDATO_NO_VERIFICADO'})
                if len(rows)>=MAX_HALLAZGOS:return rows
    return rows

def evaluar_archivo(path):
    path=Path(path)
    if not path.is_file() or path.suffix.lower()!='.pdf':raise ValueError('Archivo PDF no encontrado')
    size=path.stat().st_size
    if size>MAX_PDF_BYTES:raise ValueError('PDF mayor de 30 MiB: limite de evaluacion, sigue en cuarentena')
    with path.open('rb') as f:
        if f.read(5)!=b'%PDF-':raise ValueError('El archivo no tiene cabecera PDF valida')
    sha=hash_file(path)
    original_name=re.sub(r'^[0-9a-f]{12}_','',path.name,flags=re.I)
    total,pages=inspect_pdf(path)
    idx=[]
    f2=csv_rows('F2_menciones_auditadas.csv')
    f3=csv_rows('F3_12_cifras_corregidas_y_verificadas.csv')
    f4=csv_rows('F4_20_indicadores_narrativa_verificados.csv')
    f4other=csv_rows('F4_94_series_con_estado_revision.csv')
    conflicts=csv_rows('F4_102_conflictos_para_revision.csv')
    for row in f2:
        if sha==row.get('sha256'):
            idx.append({'fuente':'F2','pagina':int(row['pagina_pdf']),
             'indicador':row['tipo_hecho'],'valor':None,
             'estado':'MENCION_DOCUMENTAL_CORROBORADA_NO_MANDATO_APROBADO',
             'evidencia':row.get('evidencia','')[:185]})
    for row in f3:
        if sha==row.get('sha256_pdf'):
            page=int(row['pagina_pdf']);val=row['valor'];found=page<=len(pages) and has_number(pages[page-1],val)
            idx.append({'fuente':'F3','pagina':page,
             'indicador':row['indicador_corregido'],'valor':val,'periodo':row['periodo_validado'],
             'ambito':row['ambito_o_entidad'],'valor_localizado_en_pdf':found,
             'estado':'VALIDADO_POR_AUDITORIA_PREVIA' if found else 'HASH_COINCIDE_REQUIERE_COTEJO_VISUAL'})
    for row in f4:
        if sha==row.get('sha256_pdf'):
            page=int(row['pagina_pdf']);val=row['porcentaje'];found=page<=len(pages) and has_number(pages[page-1],val)
            idx.append({'fuente':'F4','pagina':page,'indicador':row['indicador'],'valor':val,
             'unidad':'porcentaje_hogares','periodo':row['periodo'],'ambito':row['ambito'],
             'valor_localizado_en_pdf':found,
             'estado':'VALIDADO_POR_AUDITORIA_PREVIA' if found else 'HASH_COINCIDE_REQUIERE_COTEJO_VISUAL'})
    for row in f4other:
        if sha==row.get('sha256_pdf') and not any(x['fuente']=='F4' and x.get('periodo')==row['periodo'] and x.get('ambito')==row['ambito'] for x in idx):
            idx.append({'fuente':'F4','pagina':int(row['pagina_pdf']), 'indicador':row['indicador'],
             'periodo':row['periodo'],'ambito':row['ambito'],'valor':row['porcentaje'],
             'estado':'PENDIENTE_VALIDACION_SEMANTICA_NO_USAR_COMO_GOLD'})
    if any(row.get('documentos') and original_name in row['documentos'].split(';') for row in conflicts):
        # Comparación por nombre solamente, no aprobacion por hash
        related=[{'periodo':r['periodo'],'ambito':r['ambito'], 'valores':r['valores']}
             for r in conflicts if original_name in r.get('documentos','').split(';')]
    else: related=[]
    checked=bool(idx)
    candidates=[] if checked else evidence_snippets(pages)
    if checked:
        families=sorted(set(x['fuente'] for x in idx))
        status='COINCIDE_CON_DOCUMENTOS_AUDITADOS_CON_RESTRICCIONES'
    else:
        families=sorted({'F4' if 'hogares' in norm(' '.join(pages[:2])) else 'F3' if 'conexiones' in norm(' '.join(pages[:2])) else 'F2' if 'mandato' in norm(' '.join(pages[:2])) else 'NO_DETERMINADA'})
        status='PDF_NUEVO_EXTRACCION_PRELIMINAR_SIN_APROBACION'
    notes=[]
    if total>MAX_PAGES:notes.append(f'Solo se extrajeron las primeras {MAX_PAGES} de {total} paginas')
    if not any(x.strip() for x in pages):notes.append('PDF sin texto extraible: requiere OCR/revision visual; no se aprobaron cifras')
    if checked:notes.append('Coincidencia exacta SHA-256 con auditoria anterior. Se heredan solo los estados limitados de cada registro.')
    else:notes.append('PDF no encontrado por SHA-256 en los registros validados; cifras son solo candidatos hasta cotejo humano.')
    if related:notes.append(f'Hay {len(related)} conflictos historicos asociados al mismo nombre documental; NO resueltos por esta revision.')
    notes.append('No se modifico HDFS, Bronze, Silver ni Gold; PDF permanece en cuarentena.')
    return {'estado':status,'nombre_archivo':original_name,'archivo_cuarentena':path.name,'sha256':sha,'tamano_bytes':size,
     'paginas_totales':total,'paginas_extraidas':len(pages),
     'familias_candidatas':families,'registros_auditados_encontrados':len(idx),
     'registros_verificados_de_auditoria':sum(x['estado']=='VALIDADO_POR_AUDITORIA_PREVIA' for x in idx),
     'hallazgos_auditoria':idx[:80], 'candidatos_nuevos':candidates,
     'conflictos_documentales_asociados':related[:30],
     'observaciones':notes, 'fecha_utc':datetime.now(timezone.utc).isoformat(),
     'aprobacion_institucional':False,'ingestado_hdfs':False}

if __name__=='__main__':
 import json,sys
 print(json.dumps(evaluar_archivo(Path(sys.argv[1])),ensure_ascii=False,indent=2))
