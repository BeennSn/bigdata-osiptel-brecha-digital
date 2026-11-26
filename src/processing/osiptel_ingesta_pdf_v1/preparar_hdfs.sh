#!/usr/bin/env bash
set -euo pipefail
NN=osiptel-bigdata-namenode-1
for target in /data/raw/pdf_controlados_v1 /data/silver/pdf_nuevos_candidatos_v1 /data/gold/pdf_documental_revisado_v1; do
  if docker exec "$NN" hdfs dfs -test -e "$target"; then
    docker exec "$NN" hdfs dfs -ls -d "$target"
  else
    docker exec "$NN" hdfs dfs -mkdir -p "$target"
    docker exec "$NN" hdfs dfs -chown spark:spark "$target"
    docker exec "$NN" hdfs dfs -chmod 755 "$target"
    docker exec "$NN" hdfs dfs -ls -d "$target"
  fi
done
