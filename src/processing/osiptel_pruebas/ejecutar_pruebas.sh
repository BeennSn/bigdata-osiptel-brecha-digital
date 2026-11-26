#!/usr/bin/env bash
set -Eeuo pipefail
cd /srv/osiptel/scripts/osiptel_pruebas
mkdir -p salida
python3 ./reparar_preflight.py
python3 ./probar_integracion.py 2>&1 | tee salida/ultima_prueba_terminal.log
rc=${PIPESTATUS[0]}
echo "CODIGO_FINAL_PRUEBAS=$rc"
exit "$rc"
