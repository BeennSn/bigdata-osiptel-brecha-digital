# OSIPTEL — Cierre de ingeniería de datos para entregar al analista

**Estado del paquete:** ejecución confirmada en Oracle mediante registro compartido por el usuario: seis tablas `PUBLICADO_Y_VERIFICADO`, `ESTADO_FINAL: COMPLETADO | codigo: 0`. Esta copia de GitHub incorpora las dos correcciones aplicadas exitosamente en Oracle (timeout HDFS 360 s; CSV locales mediante `path.as_uri()`).

## Alcance exacto

El histórico Gold F1 ya fue publicado en OCI y comprobado por el usuario. El multifuente preliminar v2 (cinco productos) también fue publicado y comprobado. Este paquete agrega una **capa documental curada distinta**, construida a partir de los PDF suministrados y de los CSV auditados.

| Producto nuevo | Filas esperadas | Conformidad y uso |
|---|---:|---|
| f2_menciones_documentales_verificadas | 287 | Menciones verificadas en PDF; **NO** indica mandatos aprobados |
| f3_cifras_contextualizadas_verificadas | 12 | Magnitudes, periodos y ámbitos corregidos; no mezclar diferencias con stocks |
| f4_hogares_internet_narrativa_verificados | 20 | 5 trimestres × 4 ámbitos; cotejo del texto explícito de PDF |
| f4_series_pendientes_revision | 74 | Valores sin discrepancia documental, pendientes de interpretación de tabla |
| f4_conflictos_excluidos | 102 | Diferencias entre publicaciones, excluidas de indicadores validados |
| comparacion_nacional_f1_f4_verificada | 5 | 4 trimestres con F1 + F4, 2026-T2 solo F4; comparación **descriptiva, no causal** |

Los nombres de tablas nuevos se publican en `/data/mart_documental_curado/<tabla>/v1_17b651ac372a96fb18fa` de forma inmutable e idempotente. **No** modifica `/data/gold`, `/data/silver`, `/data/raw` ni el mart preliminar.

**No significa que todo F2/F3/F4 tenga conformidad estadística o regulatoria institucional.** La revisión técnica es reproducible, no sustituye aprobación firmada de Calidad/OSIPTEL/INEI.

## A. Despliegue de una sola ejecución

**Para reproducir el despliegue** en WSL (Windows), descarga `OSIPTEL_CIERRE_CURADO_GITHUB_CORREGIDO.zip` y ejecuta:

```bash
scp -i ~/ssh-key-2026-03-26.key \
  /mnt/c/Users/Angel/Downloads/OSIPTEL_CIERRE_CURADO_GITHUB_CORREGIDO.zip \
  ubuntu@158.101.36.142:/srv/osiptel/scripts/
entrar-oracle
```

**Dentro de Oracle (`ubuntu@angeldev:~$`)**:

```bash
python3 -m zipfile -e \
  /srv/osiptel/scripts/OSIPTEL_CIERRE_CURADO_GITHUB_CORREGIDO.zip \
  /srv/osiptel/scripts/

bash /srv/osiptel/scripts/osiptel_cierre/desplegar.sh
```

Debe finalizar con `ESTADO_FINAL: COMPLETADO | codigo: 0` y `==== CIERRE DOCUMENTAL PUBLICADO Y VERIFICADO ====`. Si falla, **no vuelvas a ejecutar ni borres staging**; consulta `/srv/osiptel/evidence/osiptel_cierre_curado/estado_actual.json` y el log indicado.

## B. Interfaz web local, segura mediante túnel SSH

En Oracle (puede seguir funcionando el servidor antiguo en `8765`):

```bash
python3 /srv/osiptel/scripts/osiptel_cierre/interfaz_osiptel.py
```

En otra terminal **WSL**:

```bash
ssh -N -L 8766:127.0.0.1:8766 \
  -i ~/ssh-key-2026-03-26.key ubuntu@158.101.36.142
```

Abrir desde Windows **http://127.0.0.1:8766**. La interfaz permite validar, publicar o verificar de nuevo el **mismo lote revisado** y consultar estado, registro, las seis publicaciones y archivos depositados en cuarentena. No exponer 8766 al público ni abrir reglas ingress de Oracle.

**Limitación no ocultada:** los XLSX, ZIP o PDF nuevos pueden subirse a cuarentena, pero **no** se integran automáticamente a Bronze/Silver/Gold porque requieren otro esquema y revisión documental; el motor automatiza de extremo a extremo la publicación de **este lote ya auditado**, no la aprobación autónoma de documentos futuros.

## C. Verificar resultados después de publicar

```bash
docker exec osiptel-bigdata-namenode-1 sh -lc '
set -e
v=v1_17b651ac372a96fb18fa
for t in f2_menciones_documentales_verificadas f3_cifras_contextualizadas_verificadas f4_hogares_internet_narrativa_verificados f4_series_pendientes_revision f4_conflictos_excluidos comparacion_nacional_f1_f4_verificada; do
  hdfs dfs -test -e "/data/mart_documental_curado/$t/$v/_SUCCESS" || exit 1
  echo "OK $t"
done
'
```

Si hay que abrir el reporte completo: `cat /srv/osiptel/evidence/osiptel_cierre_curado/ultimo_resumen.json`.

## D. Versionar en GitHub

No se debe incluir la v1 fallida ni borrar los datos históricos. Desde Windows, puede descomprimirse este ZIP y añadirse **solo** la carpeta `osiptel_cierre` en `src/processing/osiptel_cierre/` del repositorio. Luego:

```powershell
cd C:\Users\Angel\bigdata\bigdata-osiptel-brecha-digital
# Copia primero la carpeta del ZIP extraído a src\processing\osiptel_cierre

git add -- src/processing/osiptel_cierre
git diff --cached --stat
git commit -m "feat(osiptel): cierre documental curado F2 F3 F4 y comparacion F1 F4"
git push origin feature/oci-bronze
```

No usar `git add .` ni `git push --force`.

## E. Conservación y auditoría

- CSV originales auditados inmutables y `manifest_archivos.json` con SHA-256 exactos.
- `validar_paquete.py` realiza las comprobaciones de integridad en cada corrida.
- Spark consulta F1 Gold nacional previamente publicado, verifica las cardinalidades y publica 6 snapshots inmutables, comprobados con contenido y esquema.
- El ejecutor usa un lock por corrida, bitácora y estado JSON. El fallo no elimina staging ni modifica versiones anteriores.
- Cualquier cambio a CSV genera un nuevo hash y exige nueva auditoría y regenerar el manifiesto; **no** editar los datos de este paquete para forzar su aprobación.
