#!/usr/bin/env python3
"""Interfaz local del cierre curado OSIPTEL sin dependencias externas.

Escucha solo 127.0.0.1 (tunel SSH). Procesa y publica los archivos auditados
F2/F3/F4 sobre Gold F1 existente. Las cargas nuevas quedan EN CUARENTENA;
requieren extraccion y auditoria nuevas antes de integrarse.
"""
import hashlib
import html
import json
import os
import secrets
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote

HERE = Path(__file__).resolve().parent
RUN_DIR = Path(os.getenv("OSIPTEL_RUN_DIR", "/srv/osiptel/evidence/osiptel_cierre_curado"))
UPLOAD_DIR = RUN_DIR / "cuarentena_upload"
HOST, PORT = "127.0.0.1", int(os.getenv("OSIPTEL_UI_PORT", "8766"))
TOKEN = secrets.token_urlsafe(32)
MAX_FILE = 600 * 1024 * 1024

PAGE = """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OSIPTEL - Entrega al analista</title><style>
:root{font-family:system-ui,-apple-system,Arial,sans-serif;color:#17243b;background:#f5f7fb}
body{margin:0 auto;max-width:1080px;padding:32px 20px}h1{margin:0 0 6px}
.subtitle{color:#5f6c82;margin:0 0 24px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}
.card{background:white;border:1px solid #e0e5f0;border-radius:12px;padding:18px;box-shadow:0 2px 8px #17243b09}
strong{display:block;font-size:19px}.small{font-size:13px;color:#607087}.warn{color:#8b5414}
section{margin-top:22px}.controls{display:flex;gap:10px;flex-wrap:wrap}button{background:#123d73;color:white;border:0;border-radius:8px;padding:11px 17px;cursor:pointer;font-weight:600}
button.secondary{background:#e7eef8;color:#153d6f}button:disabled{opacity:.5;cursor:wait}
pre{white-space:pre-wrap;overflow:auto;background:#10192b;color:#eff4fa;border-radius:10px;padding:15px;max-height:400px;font-size:12px}
input{padding:10px;border:1px solid #ccd7e3;border-radius:8px;max-width:100%}
.note{border-left:3px solid #d08c33;padding:10px 14px;background:#fff9f0;font-size:14px;margin:15px 0}
</style></head><body>
<h1>OSIPTEL · Entrega documental curada</h1><p class="subtitle">Cierre de ingeniería de datos | F1 Gold validado + evidencia F2/F3/F4 verificada</p>
<div class="cards"><div class="card"><strong>F1 · Conexiones</strong><div class="small">Gold histórico: 71 periodos / 129.225 filas territoriales</div></div>
<div class="card"><strong>F2 · Regulación</strong><div class="small">287 menciones documentales comprobadas</div></div>
<div class="card"><strong>F3 · Estadística</strong><div class="small">12 cifras interpretadas y cotejadas</div></div>
<div class="card"><strong>F4 · INEI</strong><div class="small">20 verificadas · 74 pendientes · 102 conflictos</div></div></div>
<div class="note"><b>Control de calidad:</b> F2 son menciones, no mandatos concedidos. F3 contiene indicadores distintos (stock, fibra, operadores, variaciones). F4: solo 20 porcentajes narrativos verificados; 74 pendientes y 102 conflictos quedan separados. No existe aprobación institucional implícita.</div>
<section class="card"><h2>Procesar paquete documental revisado + Gold histórico F1</h2><div class="controls">
<button onclick="start('preflight')">Comprobar integridad y datos</button><button onclick="start('publish')" class="secondary">Publicar seis productos curados</button>
<button onclick="refresh()" class="secondary">Actualizar estado</button></div>
<p id="result" class="small">Sin iniciar.</p><pre id="log">Cargando estado...</pre></section>
<section class="card"><h2>Productos publicados</h2><p class="small">Estado real del último cierre, con versiones HDFS.</p><pre id="summary">Sin publicación de cierre todavía.</pre></section>
<section class="card"><h2>Bandeja de nuevos archivos</h2>
<p class="small">Carga PDF, XLSX, ZIP o CSV hasta 600 MiB. Permanecen en cuarentena: NO ejecutan ingesta ni reciben aprobación automática. Para un nuevo lote se debe actualizar la extracción, revisión y versión.</p>
<input type="file" id="file" accept=".pdf,.xlsx,.zip,.csv"> <button onclick="upload()">Guardar en cuarentena</button>
<p id="upload-result" class="small"></p><pre id="uploaded"></pre></section>
<script>
const token=__TOKEN__;
async function start(mode){
  try{const r=await fetch('/api/start?mode='+mode,{method:'POST',headers:{'X-CSRF':token}});const d=await r.json();document.getElementById('result').textContent=d.mensaje||d.error;refresh()}catch(e){document.getElementById('result').textContent=e.message}
}
async function refresh(){
 try{const r=await fetch('/api/status');const d=await r.json();document.getElementById('result').textContent='Estado: '+(d.estado?.fase||'SIN EJECUCION')+' | '+(d.estado?.modo||'')+' | '+(d.estado?.log||'');document.getElementById('log').textContent=d.ultimas_lineas||'Aun sin registro';
 const s=await (await fetch('/api/summary')).json();document.getElementById('summary').textContent=s.resumen?JSON.stringify(s.resumen.publicaciones||s.resumen,null,2):'Sin publicación de cierre todavía';
 const u=await (await fetch('/api/uploads')).json();document.getElementById('uploaded').textContent=(u.archivos||[]).map(x=>x.nombre+' ('+x.bytes+' bytes)').join('\n')||'Sin archivos';
 }catch(e){document.getElementById('log').textContent=e.message}
}
async function upload(){const f=document.getElementById('file').files[0];if(!f)return;
 let el=document.getElementById('upload-result');if(f.size>600*1024*1024){el.textContent='Archivo demasiado grande';return}
 el.textContent='Subiendo...';try{const r=await fetch('/api/upload?filename='+encodeURIComponent(f.name),{method:'POST',headers:{'X-CSRF':token,'Content-Type':'application/octet-stream'},body:f});const d=await r.json();el.textContent=d.mensaje||d.error;refresh()}catch(e){el.textContent=e.message}
}
refresh();setInterval(refresh,7000);
</script></body></html>"""


def response(handler, status, data):
    encoded = json.dumps(data, ensure_ascii=False).encode("utf8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.end_headers()
    handler.wfile.write(encoded)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        request = urlparse(self.path).path
        if request == "/":
            body = PAGE.replace("__TOKEN__", json.dumps(TOKEN)).encode("utf8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        elif request == "/api/status":
            state = RUN_DIR / "estado_actual.json"
            try:
                status = json.loads(state.read_text(encoding="utf8")) if state.exists() else None
            except (ValueError, OSError):
                status = None
            logtext = ""
            if status and status.get("log"):
                p = Path(status["log"])
                if p.is_file() and p.resolve().parent == RUN_DIR.resolve():
                    # Lee solo un maximo de 32 KiB de log sin cargar el historial entero.
                    with p.open("rb") as f:
                        f.seek(0, 2)
                        n = f.tell()
                        f.seek(max(0, n - 32768))
                        logtext = f.read().decode("utf8", "replace")
            response(self, 200, {"estado": status, "ultimas_lineas": logtext[-28000:]})
        elif request == "/api/summary":
            p = RUN_DIR / "ultimo_resumen.json"
            try:
                r = json.loads(p.read_text(encoding="utf8")) if p.exists() else None
            except (ValueError, OSError):
                r = None
            response(self, 200, {"resumen": r})
        elif request == "/api/uploads":
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            files = []
            for p in sorted(UPLOAD_DIR.iterdir(), key=lambda f: f.stat().st_mtime, reverse=True)[:30]:
                if p.is_file():
                    files.append({"nombre": p.name, "bytes": p.stat().st_size})
            response(self, 200, {"archivos": files})
        else:
            response(self, 404, {"error": "Ruta inexistente"})

    def do_POST(self):
        if self.headers.get("X-CSRF") != TOKEN:
            response(self, 403, {"error": "Token incorrecto"})
            return
        u = urlparse(self.path)
        query = parse_qs(u.query)
        if u.path == "/api/start":
            mode = query.get("mode", [""])[0]
            if mode not in ("preflight", "publish"):
                response(self, 400, {"error": "Modo no permitido"})
                return
            # El orquestador usa flock para impedir concurrencia entre jobs.
            logfile = (RUN_DIR / "interfaz_lanzamientos.log")
            with logfile.open("a", encoding="utf8") as log:
                process = subprocess.Popen(
                    [sys.executable, str(HERE / "ejecutar_cierre.py"), "--" + mode],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                    cwd=str(HERE), close_fds=True)
            response(self, 202, {"mensaje": "Ejecucion solicitada. PID local: %d" % process.pid})
        elif u.path == "/api/upload":
            name = unquote(query.get("filename", [""])[0])
            name = os.path.basename(name).strip().replace("\\", "_")
            if not name or len(name) > 130 or Path(name).suffix.lower() not in (".pdf", ".xlsx", ".zip", ".csv"):
                response(self, 400, {"error": "Nombre o extension no permitidos"})
                return
            try:
                size = int(self.headers.get("Content-Length", "-1"))
            except ValueError:
                size = -1
            if size < 1 or size > MAX_FILE:
                response(self, 413, {"error": "El archivo excede el limite o esta vacio"})
                return
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            dest = UPLOAD_DIR / (uuid.uuid4().hex[:12] + "_" + name)
            h = hashlib.sha256()
            remaining = size
            with dest.open("xb") as out:
                while remaining > 0:
                    chunk = self.rfile.read(min(1024 * 1024, remaining))
                    if not chunk:
                        dest.unlink(missing_ok=True) if sys.version_info >= (3, 8) else os.unlink(str(dest))
                        response(self, 400, {"error": "Archivo recibido incompleto"})
                        return
                    out.write(chunk)
                    h.update(chunk)
                    remaining -= len(chunk)
            response(self, 201, {"mensaje": "Guardado EN CUARENTENA (sin procesar)",
                                 "archivo": dest.name, "sha256": h.hexdigest()})
        else:
            response(self, 404, {"error": "Ruta inexistente"})


if __name__ == "__main__":
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("OSIPTEL interfaz de entrega: http://127.0.0.1:%d" % PORT, flush=True)
    print("Usar tunel SSH; no exponer este puerto publicamente.", flush=True)
    server.serve_forever()
