#!/usr/bin/env bash
# Un comando: valida evidencias y publica de forma inmutable los seis productos.
set -euo pipefail
DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
echo '==== INICIO CIERRE DOCUMENTAL OSIPTEL ===='
python3 "$DIR/validar_paquete.py"
python3 "$DIR/ejecutar_cierre.py" --publish --timeout 300
echo '==== CIERRE DOCUMENTAL PUBLICADO Y VERIFICADO ===='
echo 'Panel: python3 /srv/osiptel/scripts/osiptel_cierre/interfaz_osiptel.py'
echo 'Tunel desde WSL: ssh -N -L 8766:127.0.0.1:8766 -i ~/ssh-key-2026-03-26.key ubuntu@158.101.36.142'
