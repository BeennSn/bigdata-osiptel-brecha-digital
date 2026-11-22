"""Ingesta versionada, reanudable y verificable del dataset da a HDFS.

Ejecutar desde la VM OCI (ubuntu), no desde Windows:
  python3 -u src/ingestion/ingest_da_oci.py --dry-run
  python3 -u src/ingestion/ingest_da_oci.py --repair-corrupt

Nunca borra originales. Copias incompletas en destino se conservan en cuarentena.
"""
import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

ORIGEN = Path("/srv/osiptel/staging/extraido_da/da")
ZIP = Path("/srv/osiptel/staging/da.zip")
COMPOSE = "/srv/osiptel/infra/docker-compose.yml"
MANIFEST = Path("/srv/osiptel/evidence/manifest_bronze_da.json")
LOCK = Path("/srv/osiptel/logs/ingest_da_oci.lock")
EXPECTED_COUNT = 335
EXPECTED_ZIP_SHA256 = "57cfe3ec54b1c4e33fc147abb44ea376871764b7fd89728c46a02d9ae8462790"


def sha256_local(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def hdfs_cmd(*args):
    return ["docker", "compose", "-f", COMPOSE,
            "exec", "-T", "namenode", "hdfs", "dfs", *args]


def hdfs(*args):
    subprocess.run(hdfs_cmd(*args), check=True, stdout=subprocess.DEVNULL)


def exists_hdfs(path):
    result = subprocess.run(hdfs_cmd("-test", "-e", path),
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"Error verificando HDFS: {path}: {result.stderr.decode(errors='replace')}")
    return result.returncode == 0


def hdfs_size(path):
    out = subprocess.run(hdfs_cmd("-stat", "%b", path),
                         check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return int(out.stdout.decode().strip())


def sha256_hdfs(path):
    """Lee por bloques, sin guardar el PDF entero en memoria ni bloquear stderr."""
    digest = hashlib.sha256()
    with tempfile.TemporaryFile() as error_log:
        process = subprocess.Popen(hdfs_cmd("-cat", path), stdout=subprocess.PIPE,
                                   stderr=error_log)
        assert process.stdout is not None
        with process.stdout:
            for block in iter(lambda: process.stdout.read(1024 * 1024), b""):
                digest.update(block)
        code = process.wait()
        if code:
            error_log.seek(0)
            error = error_log.read().decode(errors="replace")
            raise RuntimeError(f"No se pudo leer HDFS: {path}: {error}")
    return digest.hexdigest()


def upload_stdin(local, remote):
    with local.open("rb") as source:
        subprocess.run(hdfs_cmd("-put", "-", remote), stdin=source,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                       check=True)


def upload_or_resume(local, remote, expected_sha, version, source_id, repair):
    parent = remote.rsplit("/", 1)[0]
    hdfs("-mkdir", "-p", parent)
    quarantined = None

    if exists_hdfs(remote):
        if hdfs_size(remote) == local.stat().st_size and sha256_hdfs(remote) == expected_sha:
            return "existente", None
        if not repair:
            raise RuntimeError(
                f"Archivo diferente en {remote}. Reejecuta con --repair-corrupt "
                "para conservarlo en cuarentena y recuperar el original.")
        quarantine_base = f"/data/raw/_cuarentena/da/{version}/fuente_{source_id}"
        hdfs("-mkdir", "-p", quarantine_base)
        quarantined = f"{quarantine_base}/{uuid.uuid4().hex}_{local.name}"
        hdfs("-mv", remote, quarantined)
        print(f"  Archivo previo diferente: conservado en {quarantined}", flush=True)

    # Nunca publicar directamente en Bronze: primero cargar y verificar en staging.
    staging_dir = f"/tmp/osiptel_bronze_staging/{version}"
    hdfs("-mkdir", "-p", staging_dir)
    temporary = f"{staging_dir}/{uuid.uuid4().hex}.upload"
    try:
        upload_stdin(local, temporary)
        if hdfs_size(temporary) != local.stat().st_size:
            raise RuntimeError(f"Tamaño distinto después de cargar {local.name}")
        if sha256_hdfs(temporary) != expected_sha:
            raise RuntimeError(f"SHA-256 distinto después de cargar {local.name}")
        if exists_hdfs(remote):
            raise RuntimeError(f"El destino apareció durante la carga: {remote}")
        hdfs("-mv", temporary, remote)
    except Exception:
        print(f"  Falló la carga de {local.name}; temporal HDFS: {temporary}", flush=True)
        raise
    return ("recuperado" if quarantined else "cargado"), quarantined


def inventory():
    all_files = []
    for number in range(1, 5):
        folder = ORIGEN / f"Fuente {number}"
        if not folder.is_dir():
            raise FileNotFoundError(f"No existe {folder}")
        files = sorted(p for p in folder.rglob("*")
                       if p.is_file() and p.name != ".DS_Store")
        print(f"Fuente {number}: {len(files)} archivos encontrados", flush=True)
        for path in files:
            all_files.append((number, path, path.relative_to(folder).as_posix()))
    if len(all_files) != EXPECTED_COUNT:
        raise RuntimeError(f"Se esperaban {EXPECTED_COUNT} archivos, hay {len(all_files)}")
    return all_files


def save_manifest(manifest):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".bronze-", suffix=".json", dir=MANIFEST.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(manifest, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, MANIFEST)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description="Carga íntegra y reanudable de da a HDFS")
    parser.add_argument("--dry-run", action="store_true", help="Solo inventario")
    parser.add_argument("--repair-corrupt", action="store_true",
                        help="Conservar copias diferentes en cuarentena antes de recuperarlas")
    args = parser.parse_args()

    if not ZIP.is_file() or not ORIGEN.is_dir():
        raise FileNotFoundError("Falta da.zip o staging/extraido_da/da")
    zip_sha = sha256_local(ZIP)
    if zip_sha != EXPECTED_ZIP_SHA256:
        raise RuntimeError(f"da.zip cambió; SHA-256 actual: {zip_sha}")
    version = zip_sha[:12]
    base = f"/data/raw/da/{version}"
    files = inventory()
    print(f"TOTAL: {len(files)}; versión: {version}", flush=True)
    if args.dry_run:
        print("SIMULACION EXITOSA: sin cambios en HDFS", flush=True)
        return

    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Ya existe otra ingesta Bronze en ejecución") from error

        manifest = {"fecha_utc": datetime.now(timezone.utc).isoformat(),
                    "dataset": "da", "sha256_zip": zip_sha,
                    "destino_base": base, "archivos": []}
        total = len(files)
        for index, (number, local, relative) in enumerate(files, 1):
            target = f"{base}/fuente_{number}/{relative}"
            digest = sha256_local(local)
            print(f"[{index}/{total}] Fuente {number} | {relative}", flush=True)
            state, quarantined = upload_or_resume(
                local, target, digest, version, number, args.repair_corrupt)
            print(f"  -> {state}", flush=True)
            manifest["archivos"].append({
                "fuente": f"Fuente {number}", "nombre": relative,
                "bytes": local.stat().st_size, "sha256": digest,
                "ruta_hdfs": target, "estado": state,
                "cuarentena_anterior": quarantined})

        save_manifest(manifest)
        print(f"INGESTA BRONZE EXITOSA: {total} archivos", flush=True)
        print(f"Manifiesto: {MANIFEST}", flush=True)


if __name__ == "__main__":
    main()
