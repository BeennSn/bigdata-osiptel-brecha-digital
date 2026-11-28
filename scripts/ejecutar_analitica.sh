
#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROYECTO="$PWD"
BASE="$PROYECTO/data/paquete_20261125_c5cc727"

NACIONAL="${OSIPTEL_F1_NACIONAL:-file://$BASE/f1_historico_nacional}"
TERRITORIAL="${OSIPTEL_F1_TERRITORIAL:-file://$BASE/f1_historico_territorial}"
HOGARES="${OSIPTEL_F4:-file://$BASE/f4_hogares_verificados}"
MODELO="${OSIPTEL_MODELO:-file://$PROYECTO/models/random_forest_operativo}"

MASTER="${SPARK_MASTER:-local[2]}"

SALIDA="$PROYECTO/results/pipeline_analitico"

mkdir -p "$SALIDA"

RUN=$(mktemp -d "$SALIDA/ejecucion.XXXXXXXX")

echo "======================================"
echo "PIPELINE ANALITICO OSIPTEL"
echo "======================================"

echo "[FASE 1] Inferencia predictiva"

spark-submit --master "$MASTER" \
  src/analitica/inferencia.py \
  --gold "$TERRITORIAL" \
  --modelo "$MODELO" \
  --salida "$RUN/inferencia"

echo "[FASE 2] Modelo dimensional e indicadores"

spark-submit --master "$MASTER" \
  src/analitica/consumo.py \
  --nacional "$NACIONAL" \
  --territorial "$TERRITORIAL" \
  --hogares "$HOGARES" \
  --predicciones "file://$RUN/inferencia/fact_predicciones" \
  --salida "$RUN/consumo"

echo "[FASE 3] Publicacion de resultados"

test -s "$RUN/inferencia/predicciones.json"
test -s "$RUN/consumo/kpis.json"

ln -s "$RUN" "$RUN/enlace"
mv -Tf "$RUN/enlace" "$SALIDA/actual"

echo "======================================"
echo "PIPELINE ANALITICO COMPLETADO"
echo "======================================"
echo "Resultados: $SALIDA/actual"
