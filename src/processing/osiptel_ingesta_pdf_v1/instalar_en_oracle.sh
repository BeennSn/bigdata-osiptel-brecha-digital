#!/usr/bin/env bash
set -euo pipefail
BASE=/srv/osiptel/scripts/osiptel_ingesta_pdf_v1
python3 -m py_compile "$BASE"/*.py
bash "$BASE/preparar_hdfs.sh"
python3 "$BASE/instalar_integracion.py"
echo 'INSTALACION_COMPLETA: reiniciar exclusivamente el servidor del panel :8767'
echo 'PANEL: python3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py'
