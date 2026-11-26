#!/usr/bin/env python3
"""Agrega flujo controlado al panel activo, sin reescribir sus rutas anteriores."""
from pathlib import Path
import py_compile, shutil, sys
HERE=Path(__file__).resolve().parent
BASE=Path('/srv/osiptel/scripts/osiptel_pruebas')
PANEL=BASE/'panel_pruebas.py'
MARK='OSIPTEL_NUEVA_INGESTA_CONTROLADA_V1'
if not PANEL.is_file():sys.exit('ERROR: panel no existe. Debe ejecutarse en Oracle.')
if not (BASE/'evaluar_pdf.py').is_file():sys.exit('ERROR: primero instalar el evaluador PDF v2')
for filename in ('controlar_pdf.py','publicar_pdf_spark.py','web_routes.py'):
    py_compile.compile(str(HERE/filename),doraise=True)
code=PANEL.read_text(encoding='utf8')
if MARK in code:sys.exit('INTEGRACION_YA_INSTALADA: no se modifico el panel')
required=("HTML=r'''",'    u=urlparse(self.path);route=u.path',
          '    u=urlparse(self.path);query=parse_qs(u.query)',
          "   const r=await json('/api/evaluar-pdf?file='+encodeURIComponent(u.archivo)",
          'refresh();setInterval(refresh,6000);', '</script></body></html>')
for key in required:
    if code.count(key)!=1:sys.exit('ERROR: panel distinto del esperado en '+repr(key)[:130])

code=code.replace("HTML=r'''", "sys.path.insert(0,str(HERE.parent))\nfrom osiptel_ingesta_pdf_v1 import web_routes as rutas_nuevas\n\nHTML=r'''",1)
code=code.replace('    u=urlparse(self.path);route=u.path',
                  '    u=urlparse(self.path);route=u.path\n    if rutas_nuevas.route_get(self,u):return',1)
code=code.replace('    u=urlparse(self.path);query=parse_qs(u.query)',
                  '    u=urlparse(self.path);query=parse_qs(u.query)\n    if rutas_nuevas.route_post(self,u,query):return',1)
code=code.replace("   const r=await json('/api/evaluar-pdf?file='+encodeURIComponent(u.archivo)",
                  "   window.osiptelNuevoPDF=u.archivo;\n   const r=await json('/api/evaluar-pdf?file='+encodeURIComponent(u.archivo)",1)

section='''
<section class="panel" style="margin-top:16px" id="nuevaIngestaPanel">
 <h2>Ingreso controlado de PDF nuevos · Bronze → Silver → Gold documental</h2>
 <p class="small muted">El PDF debe haber sido evaluado arriba. Solo ingresa documentos pertinentes no auditados. Gold requiere revisión humana explícita y queda separado de los KPI oficiales.</p>
 <div class="controls"><button class="alt" onclick="nuevoIngestar()">A · Publicar Bronze + Silver candidatos</button><button class="alt" onclick="nuevoEstado()">Actualizar estado de ingesta</button></div>
 <p class="small muted" id="nuevoNombre">Primero sube y evalúa un PDF nuevo.</p>
 <pre id="nuevoProceso">Sin ingesta aún.</pre>
 <h3>B · Registrar revisión documental y publicar Gold separado</h3>
 <p class="small muted">Elige evidencia y coteja su página original. La revisión registrada no es certificación institucional ni actualiza los indicadores nacionales.</p>
 <div class="controls"><label>Evidencia candidata <select id="nuevoIndice"><option value="0">0 · Primera evidencia</option></select></label></div>
 <div class="controls"><input id="nuevoRevisor" placeholder="Responsable de revisión (obligatorio)" size="35" maxlength="100"><input id="nuevoPeriodo" placeholder="Periodo AAAA-Tn o AAAA-MM" maxlength="12"><input id="nuevoAmbito" placeholder="Ámbito / entidad" maxlength="100"></div>
 <div class="controls"><input id="nuevoValor" placeholder="Valor (solo F3/F4)"><input id="nuevoUnidad" placeholder="Unidad: porcentaje_hogares, conexiones..." size="42" maxlength="60"></div>
 <textarea id="nuevoJustificacion" rows="3" style="width:100%;padding:9px;border:1px solid #dfe5ed;border-radius:7px" maxlength="1000" placeholder="Justificación del cotejo con página y documento; al menos 25 caracteres"></textarea>
 <div class="controls"><button onclick="nuevoAprobar()">B · Registrar revisión y publicar Gold documental</button></div>
 <p class="small muted" id="nuevoMensaje">Sin acción.</p>
</section>
'''
code=code.replace('<script>\nconst token=__TOKEN__;',section+'\n<script>\nconst token=__TOKEN__;',1)

js=r'''
// OSIPTEL_NUEVA_INGESTA_CONTROLADA_V1
async function nuevoIngestar(){
 const n=window.osiptelNuevoPDF, m=document.getElementById('nuevoMensaje');
 if(!n){m.textContent='Primero sube y evalúa un PDF';return}
 try{const x=await json('/api/nuevo/ingestar?file='+encodeURIComponent(n),{method:'POST',headers:{'X-CSRF':token}});m.textContent=x.mensaje;await nuevoEstado()}
 catch(e){m.textContent='Error: '+e.message}
}
async function nuevoEstado(){
 const n=window.osiptelNuevoPDF;if(!n)return;
 try{const x=await json('/api/nuevo/estado?file='+encodeURIComponent(n));
  document.getElementById('nuevoNombre').textContent='PDF: '+n;
  document.getElementById('nuevoProceso').textContent=JSON.stringify(x.proceso,null,2);
 }catch(e){document.getElementById('nuevoMensaje').textContent='Error de consulta: '+e.message}
}
async function nuevoAprobar(){
 const n=window.osiptelNuevoPDF,m=document.getElementById('nuevoMensaje');
 if(!n){m.textContent='Primero sube y evalúa un PDF';return}
 const p={revisor:document.getElementById('nuevoRevisor').value,
  justificacion:document.getElementById('nuevoJustificacion').value,
  candidato_index:Number(document.getElementById('nuevoIndice').value),
  periodo:document.getElementById('nuevoPeriodo').value,ambito:document.getElementById('nuevoAmbito').value,
  valor:document.getElementById('nuevoValor').value,unidad:document.getElementById('nuevoUnidad').value};
 if(p.revisor.trim().length<3 || p.justificacion.trim().length<25){m.textContent='Falta revisor o justificación de 25 caracteres';return}
 if(!confirm('¿Confirmas haber contrastado la evidencia con el PDF original? Se registrará TU revisión, no una aprobación institucional.'))return;
 try{const x=await json('/api/nuevo/aprobar?file='+encodeURIComponent(n),
  {method:'POST',headers:{'X-CSRF':token,'Content-Type':'application/json'},body:JSON.stringify(p)});
  m.textContent=x.mensaje;await nuevoEstado();}
 catch(e){m.textContent='Error: '+e.message}
}
const originalUploadEval=typeof uploadEval==='function'?uploadEval:null;
if(originalUploadEval){uploadEval=async function(){await originalUploadEval();
 const n=window.osiptelNuevoPDF;
 if(n){
  document.getElementById('nuevoNombre').textContent='PDF: '+n;
  try{const x=await json('/api/evaluacion-pdf?file='+encodeURIComponent(n));
    const opts=x.evaluacion.candidatos_nuevos||[];
    const sel=document.getElementById('nuevoIndice');sel.innerHTML='';
    opts.forEach((v,i)=>{const opt=document.createElement('option');opt.value=String(i);opt.textContent=i+' · p.'+(v.pagina||'?')+' · '+(v.accion_detectada||v.tipo||'candidato');sel.appendChild(opt)});
  }catch(e){} await nuevoEstado();
 }
}}
setInterval(nuevoEstado,6000);
'''
code=code.replace('refresh();setInterval(refresh,6000);',js+'\nrefresh();setInterval(refresh,6000);',1)
backup=PANEL.with_name('panel_pruebas.py.bak_antes_ingesta_controlada')
if not backup.exists():shutil.copy2(PANEL,backup)
compile(code,str(PANEL),'exec')
PANEL.write_text(code,encoding='utf8')
py_compile.compile(str(PANEL),doraise=True)
print('INTEGRACION_CONTROLADA_INSTALADA: panel actualizado; reiniciar servidor 8767')
