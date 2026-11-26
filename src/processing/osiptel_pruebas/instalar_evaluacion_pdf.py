#!/usr/bin/env python3
"""Instala complemento sobre panel OSIPTEL sin alterar otras rutas o Gold."""
from pathlib import Path
import py_compile
import shutil
import re

BASE=Path('/srv/osiptel/scripts/osiptel_pruebas')
panel=BASE/'panel_pruebas.py'
addon=Path(__file__).resolve().parent/'evaluar_pdf.py'
if not panel.is_file():raise SystemExit('ERROR: ejecutar en Oracle, no existe panel '+str(panel))
if not addon.is_file():raise SystemExit('ERROR: falta evaluar_pdf.py junto a este instalador')
copy=BASE/'evaluar_pdf.py'
if copy.exists() and copy.read_bytes()!=addon.read_bytes():
    bk=BASE/'evaluar_pdf.py.bak_original'
    if not bk.exists():shutil.copy2(copy,bk)
if addon.resolve()!=copy.resolve():shutil.copy2(addon,copy)
py_compile.compile(str(copy),doraise=True)
text=panel.read_text(encoding='utf-8')
if 'OSIPTEL_PDF_EVAL_V1' in text:
    print('PANEL_YA_ACTUALIZADO; evaluador actualizado');raise SystemExit(0)

original='''<section class="panel"><h2>Probar recepción de nuevos archivos</h2><p class="small muted">PDF/XLSX/ZIP/CSV van solo a cuarentena; no entran automáticamente a Bronze, Silver o Gold.</p><input id="file" type="file" accept=".pdf,.xlsx,.zip,.csv"> <button class="alt" onclick="upload()">Subir a cuarentena</button><p id="uploadmsg" class="small"></p></section>'''
new='''<section class="panel"><h2>Subir y evaluar PDF</h2><p class="small muted">Compara un PDF con la auditoría de F2/F3/F4 y extrae evidencias. Un PDF nuevo queda como candidato, nunca se aprueba ni ingresa a HDFS automáticamente.</p><input id="pdfFile" type="file" accept=".pdf,application/pdf"> <button class="alt" onclick="uploadEval()">Subir y evaluar PDF</button><p id="evalmsg" class="small"></p><pre id="pdfResult" style="max-height:360px">Aún no hay evaluaciones.</pre></section>'''
if original not in text:raise SystemExit('ERROR: sección de carga del panel diferente; NO se cambió el panel')
text=text.replace(original,new,1)
js='''
// OSIPTEL_PDF_EVAL_V1
async function uploadEval(){
 const f=document.getElementById('pdfFile').files[0],m=document.getElementById('evalmsg'),o=document.getElementById('pdfResult');
 if(!f){m.textContent='Selecciona un PDF';return}
 if(!f.name.toLowerCase().endsWith('.pdf')){m.textContent='Solo PDF';return}
 m.textContent='Subiendo PDF a cuarentena...';o.textContent='';
 try{
   const u=await json('/api/upload?filename='+encodeURIComponent(f.name),{method:'POST',headers:{'X-CSRF':token,'Content-Type':'application/octet-stream'},body:f});
   m.textContent='PDF recibido; evaluando contenido...';
   const r=await json('/api/evaluar-pdf?file='+encodeURIComponent(u.archivo),{method:'POST',headers:{'X-CSRF':token}});
   const d=r.evaluacion;
   m.textContent='Evaluación terminada: '+d.estado+' | auditados: '+d.registros_auditados_encontrados+' | candidatos: '+d.candidatos_nuevos.length;
   const reduced={estado:d.estado,fuentes:d.familias_candidatas,sha256:d.sha256,paginas:d.paginas_totales,
     registros_auditados:d.registros_auditados_encontrados,verificados_automaticamente:d.registros_verificados_de_auditoria,
     hallazgos_auditoria:d.hallazgos_auditoria,candidatos:d.candidatos_nuevos,conflictos:d.conflictos_documentales_asociados,observaciones:d.observaciones};
   o.textContent=JSON.stringify(reduced,null,2);
 }catch(e){m.textContent='Error de evaluación: '+e.message;o.textContent='El archivo queda en cuarentena para revisión.'}
}
'''
needle='refresh();setInterval(refresh,6000);'
if text.count(needle)!=1:raise SystemExit('ERROR: punto de inserción JS no encontrado')
text=text.replace(needle,js+'\n'+needle,1)

# GET API para descargar el JSON después. El nombre debe ser el exacto del upload.
needle="    elif route=='/api/download':"
insert="""    elif route=='/api/evaluacion-pdf':
      name=parse_qs(u.query).get('file',[''])[0]
      if not re.fullmatch(r'[0-9a-f]{12}_[^/\\\\]{1,130}\\.pdf',name,re.I):
        return send(self,400,{'error':'Nombre de PDF no valido'})
      file=RUN/'evaluaciones_pdf'/(name+'.json')
      if not file.is_file():return send(self,404,{'error':'PDF aun no evaluado'})
      send(self,200,{'evaluacion':read_json(file)})
"""
if needle not in text:raise SystemExit('ERROR: API GET no reconocida')
text=text.replace(needle,insert+needle,1)

needle="    else:send(self,404,{'error':'Ruta inexistente'})"
# 2 occurrences: GET and POST; only POST insert before last
post="""    elif u.path=='/api/evaluar-pdf':
      name=query.get('file',[''])[0]
      if not re.fullmatch(r'[0-9a-f]{12}_[^/\\\\]{1,130}\\.pdf',name,re.I):
        return send(self,400,{'error':'Identificador PDF no valido'})
      pdf=QUARANTINE/name
      if not pdf.is_file() or pdf.is_symlink():return send(self,404,{'error':'PDF no esta en cuarentena'})
      try:
        from evaluar_pdf import evaluar_archivo
        result=evaluar_archivo(pdf)
        target=RUN/'evaluaciones_pdf'
        target.mkdir(parents=True,exist_ok=True)
        result_path=target/(name+'.json')
        temporal=target/(name+'.json.tmp')
        temporal.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\\n',encoding='utf-8')
        os.replace(temporal,result_path)
        send(self,200,{'mensaje':'PDF evaluado sin afectar HDFS','evaluacion':result,'url_resultado':'/api/evaluacion-pdf?file='+name})
      except (ValueError,RuntimeError,subprocess.TimeoutExpired) as e:
        send(self,422,{'error':str(e)[:400]})
"""
if text.count(needle)!=2:raise SystemExit('ERROR: número inesperado de finales de ruta')
pos=text.rfind(needle)
text=text[:pos]+post+text[pos:]
# agregar re import en encabezado
text=text.replace('import hashlib\n','import hashlib\nimport re\n',1)
# No tocamos OUT del pipeline
bk=panel.with_name('panel_pruebas.py.bak_antes_evaluacion_pdf')
if not bk.exists():shutil.copy2(panel,bk)
# Check sintaxis antes de escritura
compile(text,str(panel),'exec')
panel.write_text(text,encoding='utf-8')
py_compile.compile(str(panel),doraise=True)
print('INSTALACION_OK: panel actualizado y evaluador PDF disponible')
for exe in ['pdfinfo','pdftotext']:
    if not shutil.which(exe):print('ADVERTENCIA: falta '+exe+'; instalar poppler-utils antes de evaluar PDFs')
print('REINICIAR_PANEL: Ctrl+C y python3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py')
