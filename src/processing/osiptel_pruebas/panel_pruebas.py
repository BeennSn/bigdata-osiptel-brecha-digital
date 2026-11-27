#!/usr/bin/env python3
"""Panel local en http://127.0.0.1:8767, sin dependencias web externas."""
import hashlib
import re
import json
import os
import secrets
import subprocess
import sys
import uuid
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs,urlparse,unquote

HERE=Path(__file__).resolve().parent
OUT=HERE/'salida'
PREDICCIONES=Path(os.getenv(
    'OSIPTEL_PREDICCIONES_JSON',
    str(OUT/'predicciones.json')
))
RUN=Path(os.getenv('OSIPTEL_RUN_DIR','/srv/osiptel/evidence/osiptel_cierre_curado'))
QUARANTINE=RUN/'cuarentena_upload'
PORT=int(os.getenv('OSIPTEL_TEST_PANEL_PORT','8767'))
TOKEN=secrets.token_urlsafe(32)
MAX_SIZE=600*1024*1024

sys.path.insert(0,str(HERE.parent))
from osiptel_ingesta_pdf_v1 import web_routes as rutas_nuevas

HTML=r'''<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OSIPTEL | Prueba integral y decisiones</title>
<style>
:root{--bg:#f4f7fb;--ink:#1d2a3b;--secondary:#607186;--border:#dfe5ed;--blue:#174978;--green:#166c4a;--white:#fff;font-family:system-ui,-apple-system,Arial,sans-serif;color:var(--ink);background:var(--bg)}
*{box-sizing:border-box}body{margin:0 auto;max-width:1250px;padding:26px 20px 60px}h1{margin:0;font-size:26px}h2{margin:0 0 14px;font-size:19px}h3{margin:0 0 8px;font-size:16px}p{line-height:1.5}.muted{color:var(--secondary)}.small{font-size:12px}header{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;margin:0 0 20px}.version{font-size:12px;color:var(--secondary)}
.panel,.stat,.decision{border:1px solid var(--border);border-radius:12px;background:white;padding:18px}.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:18px}.stat label{display:block;color:var(--secondary);font-size:13px;margin-bottom:8px}.stat strong{font-size:26px}.layout{display:grid;grid-template-columns:1.5fr 1fr;gap:16px}.stack{display:grid;gap:16px}.controls{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
button{border:0;border-radius:7px;padding:10px 14px;color:white;background:var(--blue);font-weight:600;cursor:pointer}button.alt{background:#e8f0f8;color:#16436e}button:disabled{opacity:.5;cursor:wait}a{color:var(--blue)}.tag{display:inline-block;border-radius:99px;background:#e5f2eb;color:#126c47;font-size:11px;font-weight:700;padding:4px 9px}.tag.warn{background:#fff1d7;color:#8c5703}.tag.low{background:#eaf1fa;color:#315f91}
.decision{margin-bottom:9px;padding:14px 16px;border-left:4px solid #b9cadb}.decision p{font-size:13px;margin:6px 0}.decision b{font-size:14px}.grid4{display:grid;gap:9px}.barrow{display:grid;grid-template-columns:155px 1fr 52px;gap:9px;align-items:center;font-size:13px;margin:8px 0}.bartrack{height:13px;border-radius:6px;background:#e9eef3;overflow:hidden}.fill{height:100%;border-radius:6px;background:#2d7d9e}.fill.rural{background:#d89a3a}.fill.lima{background:#244c83}.fill.nacional{background:#508b71}
.tablebox{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:9px;border-bottom:1px solid #ebeff4;white-space:nowrap}th{font-size:12px;color:#65778c}.negative{color:#aa342f;font-weight:650}.positive{color:#176c47;font-weight:650}pre{background:#162234;border-radius:9px;color:#e8f2fa;padding:13px;white-space:pre-wrap;overflow:auto;max-height:290px;font-size:11px}input{padding:8px;border:1px solid var(--border);border-radius:7px;max-width:100%}.hint{border-left:4px solid #d9a24f;background:#fff7e9;padding:12px 15px;border-radius:4px;font-size:13px}.fail{color:#9f2424}.ok{color:#176c47}
@media(max-width:850px){.layout{grid-template-columns:1fr}.barrow{grid-template-columns:115px 1fr 48px}body{padding:14px}}
</style></head><body>
<header><div><h1>OSIPTEL · Laboratorio de decisiones</h1><p class="muted" style="margin:4px 0">Prueba integral · HDFS/Spark · F1/F2/F3/F4 · datos revisados</p></div><div class="version">Gold F1 v2 · Mart documental curado v1<br>Interfaz restringida a 127.0.0.1 por túnel SSH</div></header>
<div class="stats"><div class="stat"><label>Estado pruebas automáticas</label><strong id="testState">Sin ejecutar</strong><div class="small muted" id="testCount"></div></div><div class="stat"><label>Hogares internet rural (INEI)</label><strong id="rural">—</strong><div class="small muted" id="periodoF4">Esperando Spark</div></div><div class="stat"><label>Brecha rural–Lima</label><strong id="gap">—</strong><div class="small muted">Puntos porcentuales, no conexiones</div></div><div class="stat"><label>Variación nacional F1</label><strong id="f1">—</strong><div class="small muted" id="periodoF1">Cierres trimestrales</div></div></div>
<div class="panel" style="margin-bottom:16px"><h2>Ejecutar y validar el pipeline completo</h2><div class="controls"><button id="allBtn" onclick="start('todo')">1 · Probar todo (6 controles)</button><button class="alt" onclick="start('decisiones')">2 · Actualizar indicadores desde HDFS</button><button class="alt" onclick="start('preflight')">Solo preflight</button><button class="alt" onclick="start('publish')">Repetir publicación (idempotencia)</button><button class="alt" onclick="refresh()">Actualizar resultados</button></div>
<p id="runState" class="small muted">Sin acción activa.</p><a href="/api/download?file=pruebas">Descargar informe de pruebas JSON</a> · <a href="/api/download?file=decisiones">Descargar decisiones JSON</a>
<details style="margin-top:10px"><summary>Ver últimas líneas de la ejecución</summary><pre id="log">Sin registros.</pre></details>
<div id="checks" class="small"></div></div>
<div class="hint" style="margin-bottom:16px"><strong>Alcance correcto:</strong> 287 menciones F2 no son mandatos aprobados. Las 12 cifras F3 tienen unidades distintas. F4 incluye solo 20 indicadores narrativos corroborados; 74 series pendientes y 102 conflictos no se usan para decidir. La carga de documentos nuevos permanece en cuarentena, sin ingesta automática.</div>
<div class="layout"><div class="stack"><section class="panel"><h2>Decisiones sugeridas a partir de datos reales</h2><div id="decisiones" class="small muted">Haz clic en «Actualizar indicadores desde HDFS» para generar un reporte verificable.</div></section>
<section class="panel"><h2>Acceso a internet en hogares · INEI</h2><p class="small muted" id="ineiInfo">Porcentajes por ámbito, según narrativas de PDF corroboradas.</p><div id="ineibars" class="grid4"></div><div class="tablebox"><table><thead><tr><th>Trimestre</th><th>Nacional</th><th>Lima</th><th>Resto urbano</th><th>Rural</th></tr></thead><tbody id="ineitbl"></tbody></table></div></section>
<section class="panel"><h2>Departamentos que ameritan investigación · OSIPTEL F1</h2><p class="small muted">Cambios en stock de conexiones fijas, no en cobertura geográfica. Ordenados por caída relativa; base previa ≥1.000.</p><div class="tablebox"><table><thead><tr><th>Departamento</th><th>Antes</th><th>Después</th><th>Variación</th><th>Diferencia</th></tr></thead><tbody id="regiones"></tbody></table></div><p class="small muted" id="regionNote"></p></section></div>
<div class="stack"><section class="panel"><h2>Evolución nacional F1 vs F4</h2><p class="small muted">Indicadores con distintas unidades. Comparación descriptiva, no una resta ni relación causal.</p><div class="tablebox"><table><thead><tr><th>Periodo</th><th>F1 conexiones</th><th>F4 hogares %</th></tr></thead><tbody id="comparacion"></tbody></table></div></section>
<section class="panel"><h2>Fuente 2 · Tipos de menciones</h2><div id="tiposF2" class="small muted">Sin consultar datos.</div></section>
<section class="panel"><h2>Fuente 3 · Estadística contextualizada</h2><div class="tablebox"><table><thead><tr><th>Periodo</th><th>Entidad / tipo</th><th>Valor</th></tr></thead><tbody id="f3tbl"></tbody></table></div></section>
<section class="panel"><h2>Subir y evaluar PDF</h2><p class="small muted">Compara un PDF con la auditoría de F2/F3/F4 y extrae evidencias. Un PDF nuevo queda como candidato, nunca se aprueba ni ingresa a HDFS automáticamente.</p><input id="pdfFile" type="file" accept=".pdf,application/pdf"> <button class="alt" onclick="uploadEval()">Subir y evaluar PDF</button><p id="evalmsg" class="small"></p><pre id="pdfResult" style="max-height:360px">Aún no hay evaluaciones.</pre></section></div></div>


<section class="panel" style="margin-top:16px" id="panelPredicciones">
<h2>Analítica predictiva · Random Forest</h2>

<p class="small muted" id="predMeta">
Esperando resultados de inferencia.
</p>

<div class="stats">
  <div class="stat">
    <label>Conexiones observadas</label>
    <strong id="predObservado">—</strong>
  </div>
  <div class="stat">
    <label>Conexiones estimadas</label>
    <strong id="predEstimado">—</strong>
  </div>
  <div class="stat">
    <label>Variación trimestral estimada</label>
    <strong id="predVariacion">—</strong>
  </div>
  <div class="stat">
    <label>Territorios procesados</label>
    <strong id="predTerritorios">—</strong>
  </div>
</div>

<h3>Variación estimada por territorio</h3>

<p class="small muted">
Las barras muestran la magnitud del cambio.
Rojo: disminución. Verde: crecimiento.
</p>

<div id="predBarras"></div>

<h3>Detalle de predicciones</h3>

<div class="tablebox">
<table>
<thead>
<tr>
  <th>Departamento</th>
  <th>Observado</th>
  <th>Estimado</th>
  <th>Diferencia</th>
  <th>Variación</th>
</tr>
</thead>
<tbody id="predTabla"></tbody>
</table>
</div>

<p class="hint" id="predAdvertencia">
Las estimaciones no representan valores confirmados.
</p>

<p class="small muted" id="predEstado"></p>
</section>

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

<script>
const token=__TOKEN__;const fmt=n=>n==null?'—':new Intl.NumberFormat('es-PE').format(n);const pct=n=>n==null?'—':n.toFixed(1)+' %';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let payload=null;
async function json(url,opts){const r=await fetch(url,opts);let x=await r.json();if(!r.ok)throw Error(x.error||('HTTP '+r.status));return x}
async function start(task){try{let r=await json('/api/run?task='+task,{method:'POST',headers:{'X-CSRF':token}});document.getElementById('runState').textContent=r.mensaje;refresh()}catch(e){document.getElementById('runState').textContent='Error: '+e.message}}
function chartValue(area,val){let cls=area.includes('rural')?'rural':area.includes('Lima')?'lima':area==='Nacional'?'nacional':'';return `<div class="barrow"><span>${esc(area)}</span><div class="bartrack"><div class="fill ${cls}" style="width:${Math.max(0,Math.min(100,val))}%"></div></div><b>${pct(val)}</b></div>`}
function render(d){payload=d;const inf=d.inei,ns=d.osiptel_f1,serie=inf.series,latest=inf.ultimo_periodo;
 document.getElementById('rural').textContent=pct(serie[latest]['Area rural']);document.getElementById('gap').textContent=inf.brecha_rural_lima_pp.toFixed(1)+' pp';document.getElementById('periodoF4').textContent='INEI '+latest;
 document.getElementById('f1').textContent=(ns.variacion_pct>0?'+':'')+ns.variacion_pct.toFixed(2)+' %';document.getElementById('periodoF1').textContent=ns.periodo_anterior+' → '+ns.periodo_actual;
 document.getElementById('ineibars').innerHTML=['Lima Metropolitana','Nacional','Resto urbano','Area rural'].map(k=>chartValue(k,serie[latest][k])).join('');
 document.getElementById('ineitbl').innerHTML=Object.entries(serie).sort((a,b)=>a[0].localeCompare(b[0])).map(([p,v])=>`<tr><td>${esc(p)}</td><td>${pct(v['Nacional'])}</td><td>${pct(v['Lima Metropolitana'])}</td><td>${pct(v['Resto urbano'])}</td><td>${pct(v['Area rural'])}</td></tr>`).join('');
 document.getElementById('decisiones').innerHTML=d.decisiones.map(x=>`<article class="decision"><span class="tag ${x.prioridad==='MEDIA'?'low':''}">${esc(x.prioridad)}</span> <b>${esc(x.codigo)} · ${esc(x.titulo)}</b><p><strong>Hallazgo:</strong> ${esc(x.indicador)}</p><p><strong>Acción:</strong> ${esc(x.accion)}</p><p class="muted"><strong>Fuente:</strong> ${esc(x.evidencia)}<br><strong>Límite:</strong> ${esc(x.limite)}</p></article>`).join('');
 document.getElementById('regiones').innerHTML=(ns.prioridad_revision_departamental||[]).slice(0,10).map(x=>`<tr><td>${esc(x.departamento)}</td><td>${fmt(x.anterior)}</td><td>${fmt(x.actual)}</td><td class="negative">${x.variacion_pct.toFixed(2)}%</td><td class="negative">${fmt(x.diferencia)}</td></tr>`).join('')||'<tr><td colspan="5">No hay caídas con base suficiente.</td></tr>';
 document.getElementById('regionNote').textContent='Departamentos analizados: '+ns.departamentos_analizados+'. Con caídas y base ≥1.000: '+ns.departamentos_con_caida_y_base_minima+'. Periodos '+ns.periodo_anterior+' y '+ns.periodo_actual+'.';
 document.getElementById('comparacion').innerHTML=d.comparacion.map(x=>`<tr><td>${esc(x.periodo)}</td><td>${fmt(x.conexiones_fijas)}</td><td>${pct(x.hogares_pct)}</td></tr>`).join('');
 document.getElementById('tiposF2').innerHTML=d.regulacion_f2.map(x=>`<div class="barrow"><span>${esc(x.tipo).replaceAll('_',' ')}</span><div class="bartrack"><div class="fill" style="width:${(100*x.menciones/287).toFixed(1)}%"></div></div><b>${x.menciones}</b></div>`).join('');
 document.getElementById('f3tbl').innerHTML=d.estadisticas_f3.map(x=>`<tr title="${esc(x.documento)} p.${x.pagina}"><td>${esc(x.periodo)}</td><td>${esc(x.entidad)} · ${esc(x.naturaleza)}</td><td>${fmt(x.valor)}</td></tr>`).join('');
}

async function refreshPredicciones(){
  const estado=document.getElementById('predEstado');

  try{
    const respuesta=await json('/api/predicciones');
    const p=respuesta.predicciones;

    if(!p){
      estado.textContent='Todavía no existe una inferencia publicada.';
      return;
    }

    const signed=n=>n==null?'—':(n>0?'+':'')+n.toFixed(2)+'%';
    const cls=n=>n<0?'negative':'positive';
    const num=n=>fmt(Math.round(n));

    document.getElementById('predMeta').textContent=
      'Modelo: '+p.modelo+
      ' | Entrenamiento hasta: '+p.fin_entrenamiento+
      ' | Origen: '+p.periodo_origen+
      ' | Predicción: '+p.periodo_prediccion;

    document.getElementById('predObservado').textContent=
      num(p.conexiones_nacionales_observadas);

    document.getElementById('predEstimado').textContent=
      num(p.conexiones_nacionales_estimadas);

    const v=document.getElementById('predVariacion');

    v.textContent=signed(p.variacion_nacional_estimada_pct);
    v.className=cls(p.variacion_nacional_estimada_pct);

    document.getElementById('predTerritorios').textContent=
      p.territorios;

    const filas=[...p.predicciones].sort(
      (a,b)=>a.variacion_estimada_pct-b.variacion_estimada_pct
    );

    const max=Math.max(
      1,...filas.map(x=>Math.abs(x.variacion_estimada_pct||0))
    );

    document.getElementById('predBarras').innerHTML=
      filas.map(x=>{
        const valor=x.variacion_estimada_pct;
        const ancho=Math.abs(valor||0)/max*100;
        const color=valor<0?'#aa342f':'#166c4a';

        return `<div class="barrow">
          <span>${esc(x.departamento)}</span>
          <div class="bartrack">
            <div class="fill"
              style="width:${ancho}%;background:${color}">
            </div>
          </div>
          <b class="${cls(valor)}">${signed(valor)}</b>
        </div>`;
      }).join('');

    document.getElementById('predTabla').innerHTML=
      filas.map(x=>`<tr>
        <td>${esc(x.departamento)}</td>
        <td>${num(x.conexiones_observadas)}</td>
        <td>${num(x.conexiones_predichas)}</td>
        <td>${num(x.variacion_estimada)}</td>
        <td class="${cls(x.variacion_estimada_pct)}">
          ${signed(x.variacion_estimada_pct)}
        </td>
      </tr>`).join('');

    document.getElementById('predAdvertencia').textContent=
      p.advertencia+
      ' Los valores predichos deben contrastarse con los próximos registros de OSIPTEL.';

    estado.textContent='Inferencia generada: '+p.generado_utc;

  }catch(e){
    estado.textContent='Error al consultar inferencia: '+e.message;
  }
}

async function refresh(){refreshPredicciones();try{const [st,rep,dec]=await Promise.all([json('/api/run-status'),json('/api/pruebas'),json('/api/decisiones')]);let x=st.estado||{};document.getElementById('runState').textContent='Acción '+(x.tarea||'ninguna')+': '+(x.fase||'SIN EJECUTAR')+' · '+(x.error||'');document.getElementById('log').textContent=st.log||'Sin registros';const r=rep.reporte;
 if(r){const n=r.pruebas.filter(x=>x.estado==='OK').length;document.getElementById('testState').textContent=r.estado==='APROBADO_TECNICAMENTE'?'6 / 6 OK':r.estado==='ERROR_PRUEBA'?'ERROR':n+' / 6';document.getElementById('testCount').textContent='Pruebas completadas '+n+'/6';document.getElementById('checks').innerHTML=r.pruebas.map(x=>`<p class="small ${x.estado==='OK'?'ok':'fail'}">${x.estado==='OK'?'✓':'✕'} ${esc(x.prueba)} · ${esc(x.estado)} (${x.segundos}s)</p>`).join('');}
 if(dec.decisions && (!payload || dec.decisions.generado_utc!==payload.generado_utc))render(dec.decisions);
 }catch(e){document.getElementById('runState').textContent='Error al leer estado: '+e.message}}
async function upload(){const f=document.getElementById('file').files[0];if(!f)return;const msg=document.getElementById('uploadmsg');msg.textContent='Subiendo a cuarentena...';try{const x=await json('/api/upload?filename='+encodeURIComponent(f.name),{method:'POST',headers:{'X-CSRF':token,'Content-Type':'application/octet-stream'},body:f});msg.textContent=x.mensaje+' · SHA-256 '+x.sha256.slice(0,16)+'…'}catch(e){msg.textContent='Error: '+e.message}}

// OSIPTEL_PDF_EVAL_V1
async function uploadEval(){
 const f=document.getElementById('pdfFile').files[0],m=document.getElementById('evalmsg'),o=document.getElementById('pdfResult');
 if(!f){m.textContent='Selecciona un PDF';return}
 if(!f.name.toLowerCase().endsWith('.pdf')){m.textContent='Solo PDF';return}
 m.textContent='Subiendo PDF a cuarentena...';o.textContent='';
 try{
   const u=await json('/api/upload?filename='+encodeURIComponent(f.name),{method:'POST',headers:{'X-CSRF':token,'Content-Type':'application/octet-stream'},body:f});
   m.textContent='PDF recibido; evaluando contenido...';
   window.osiptelNuevoPDF=u.archivo;
   const r=await json('/api/evaluar-pdf?file='+encodeURIComponent(u.archivo),{method:'POST',headers:{'X-CSRF':token}});
   const d=r.evaluacion;
   m.textContent='Evaluación terminada: '+d.estado+' | auditados: '+d.registros_auditados_encontrados+' | candidatos: '+d.candidatos_nuevos.length;
   const reduced={estado:d.estado,fuentes:d.familias_candidatas,sha256:d.sha256,paginas:d.paginas_totales,
     registros_auditados:d.registros_auditados_encontrados,verificados_automaticamente:d.registros_verificados_de_auditoria,
     hallazgos_auditoria:d.hallazgos_auditoria,candidatos:d.candidatos_nuevos,conflictos:d.conflictos_documentales_asociados,observaciones:d.observaciones};
   o.textContent=JSON.stringify(reduced,null,2);
 }catch(e){m.textContent='Error de evaluación: '+e.message;o.textContent='El archivo queda en cuarentena para revisión.'}
}


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

refresh();setInterval(refresh,6000);
</script></body></html>'''

def send(h,status,obj):
  body=json.dumps(obj,ensure_ascii=False).encode('utf-8');h.send_response(status)
  h.send_header('Content-Type','application/json; charset=utf-8')
  h.send_header('Cache-Control','no-store');h.send_header('Content-Length',str(len(body)))
  h.end_headers();h.wfile.write(body)

def read_json(path):
  try:return json.loads(path.read_text(encoding='utf-8')) if path.is_file() else None
  except (OSError,ValueError):return None

def log_tail(path,n=32000):
  if not path.is_file():return ''
  with path.open('rb') as f:
    f.seek(0,os.SEEK_END);f.seek(max(0,f.tell()-n));return f.read().decode('utf-8','replace')

class Handler(BaseHTTPRequestHandler):
  def do_GET(self):
    u=urlparse(self.path);route=u.path
    if rutas_nuevas.route_get(self,u):return
    if route=='/':
      data=HTML.replace('__TOKEN__',json.dumps(TOKEN)).encode('utf-8');self.send_response(200)
      self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(data)))
      self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(data)
    elif route=='/api/run-status':
      state=read_json(OUT/'estado_panel.json')
      send(self,200,{'estado':state,'log':log_tail(OUT/'accion_ultima.log')})
    elif route=='/api/pruebas':send(self,200,{'reporte':read_json(OUT/'reporte_pruebas.json')})
    elif route=='/api/decisiones':send(self,200,{'decisions':read_json(OUT/'decisiones.json')})
    elif route=='/api/predicciones':
      send(self,200,{'predicciones':read_json(PREDICCIONES)})
    elif route=='/api/uploads':
      QUARANTINE.mkdir(parents=True,exist_ok=True)
      paths=sorted((x for x in QUARANTINE.iterdir() if x.is_file()),key=lambda x:x.stat().st_mtime,reverse=True)[:25]
      send(self,200,{'archivos':[{'nombre':x.name,'bytes':x.stat().st_size} for x in paths]})
    elif route=='/api/evaluacion-pdf':
      name=parse_qs(u.query).get('file',[''])[0]
      if not re.fullmatch(r'[0-9a-f]{12}_[^/\\]{1,130}\.pdf',name,re.I):
        return send(self,400,{'error':'Nombre de PDF no valido'})
      file=RUN/'evaluaciones_pdf'/(name+'.json')
      if not file.is_file():return send(self,404,{'error':'PDF aun no evaluado'})
      send(self,200,{'evaluacion':read_json(file)})
    elif route=='/api/download':
      key=parse_qs(u.query).get('file',[''])[0]
      if key not in ['pruebas','decisiones']:return send(self,404,{'error':'Archivo no permitido'})
      path=OUT/('reporte_pruebas.json' if key=='pruebas' else 'decisiones.json')
      if not path.is_file():return send(self,404,{'error':'Archivo no generado'})
      raw=path.read_bytes();self.send_response(200)
      self.send_header('Content-Type','application/json; charset=utf-8')
      self.send_header('Content-Disposition','attachment; filename="'+path.name+'"')
      self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    else:send(self,404,{'error':'Ruta inexistente'})

  def do_POST(self):
    if self.headers.get('X-CSRF')!=TOKEN:return send(self,403,{'error':'CSRF invalido'})
    u=urlparse(self.path);query=parse_qs(u.query)
    if rutas_nuevas.route_post(self,u,query):return
    if u.path=='/api/run':
      task=query.get('task',[''])[0]
      if task not in ('todo','decisiones','preflight','publish'):
        return send(self,400,{'error':'Accion no valida'})
      state=read_json(OUT/'estado_panel.json')
      if state and state.get('fase')=='EJECUTANDO':
        return send(self,409,{'error':'Ya hay una accion activa; actualice estado'})
      with (OUT/'lanzamientos_panel.log').open('ab') as log:
        proc=subprocess.Popen([sys.executable,str(HERE/'accion.py'),'--task',task],
              cwd=str(HERE),stdout=log,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
      send(self,202,{'mensaje':'Acción solicitada: '+task+' · PID '+str(proc.pid)})
    elif u.path=='/api/upload':
      name=os.path.basename(unquote(query.get('filename',[''])[0]).strip()).replace('\\','_')
      if not name or len(name)>130 or Path(name).suffix.lower() not in ('.pdf','.xlsx','.zip','.csv'):
        return send(self,400,{'error':'Extensión o nombre inválido'})
      try:size=int(self.headers.get('Content-Length','-1'))
      except ValueError:size=-1
      if size<1 or size>MAX_SIZE:return send(self,413,{'error':'Tamaño no permitido'})
      QUARANTINE.mkdir(parents=True,exist_ok=True)
      dest=QUARANTINE/(uuid.uuid4().hex[:12]+'_'+name)
      digest=hashlib.sha256();remaining=size
      with dest.open('xb') as out:
        while remaining>0:
          chunk=self.rfile.read(min(1024*1024,remaining))
          if not chunk:
            dest.unlink()
            return send(self,400,{'error':'Archivo incompleto'})
          out.write(chunk);digest.update(chunk);remaining-=len(chunk)
      send(self,201,{'mensaje':'Archivo guardado EN CUARENTENA (NO procesado)','archivo':dest.name,'sha256':digest.hexdigest()})
    elif u.path=='/api/evaluar-pdf':
      name=query.get('file',[''])[0]
      if not re.fullmatch(r'[0-9a-f]{12}_[^/\\]{1,130}\.pdf',name,re.I):
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
        temporal.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        os.replace(temporal,result_path)
        send(self,200,{'mensaje':'PDF evaluado sin afectar HDFS','evaluacion':result,'url_resultado':'/api/evaluacion-pdf?file='+name})
      except (ValueError,RuntimeError,subprocess.TimeoutExpired) as e:
        send(self,422,{'error':str(e)[:400]})
    else:send(self,404,{'error':'Ruta inexistente'})

if __name__=='__main__':
  OUT.mkdir(parents=True,exist_ok=True)
  server=ThreadingHTTPServer(('127.0.0.1',PORT),Handler)
  print('Panel OSIPTEL pruebas y decisiones: http://127.0.0.1:%d'%PORT,flush=True)
  print('Solo loopback; usar tunel SSH',flush=True)
  server.serve_forever()
