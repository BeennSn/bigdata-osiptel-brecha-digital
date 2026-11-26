#!/usr/bin/env bash
# Crea SOLO las seis carpetas nuevas de marts curados; no cambia Bronze/Silver/Gold ni marts v2.
set -euo pipefail
NN=osiptel-bigdata-namenode-1
ROOT=/data/mart_documental_curado
TABLES=(
 f2_menciones_documentales_verificadas
 f3_cifras_contextualizadas_verificadas
 f4_hogares_internet_narrativa_verificados
 f4_series_pendientes_revision
 f4_conflictos_excluidos
 comparacion_nacional_f1_f4_verificada
)
docker exec "$NN" hdfs dfs -mkdir -p "$ROOT"
for table in "${TABLES[@]}"; do
  folder="$ROOT/$table"
  if docker exec "$NN" hdfs dfs -test -e "$folder"; then
    echo "YA_EXISTE $folder"
    # Un directorio existente no se reasigna a ciegas.
    docker exec "$NN" hdfs dfs -ls -d "$folder"
  else
    docker exec "$NN" hdfs dfs -mkdir "$folder"
    docker exec "$NN" hdfs dfs -chown spark:spark "$folder"
    docker exec "$NN" hdfs dfs -chmod 755 "$folder"
    docker exec "$NN" hdfs dfs -ls -d "$folder"
  fi
done
