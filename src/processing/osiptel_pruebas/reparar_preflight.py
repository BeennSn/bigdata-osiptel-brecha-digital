#!/usr/bin/env python3
"""Evita que preflight borre de la vista el resumen de la publicación existente."""
from pathlib import Path
p=Path('/srv/osiptel/scripts/osiptel_cierre/ejecutar_cierre.py')
if not p.is_file():raise SystemExit('ERROR: ejecutar dentro de Oracle con scripts del cierre instalados')
s=p.read_text(encoding='utf-8')
old='            if marker in full:\n'
new='            if marker in full and args.publish:\n'
if old in s:
    if s.count(old)!=1: raise SystemExit('ERROR: patron inesperado; no se modifica')
    backup=p.with_suffix('.py.bak_antes_pruebas')
    if not backup.exists():backup.write_text(s,encoding='utf-8')
    p.write_text(s.replace(old,new,1),encoding='utf-8')
    print('AJUSTE_OK: preflight conserva el ultimo resumen publicado')
elif new in s:
    print('AJUSTE_YA_APLICADO')
else:raise SystemExit('ERROR: version del ejecutor desconocida; no se modifica')
