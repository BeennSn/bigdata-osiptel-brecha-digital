
import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ORIGEN = Path("/srv/osiptel/staging/extraido_da/da")
ZIP = Path("/srv/osiptel/staging/da.zip")
COMPOSE = "/srv/osiptel/infra/docker-compose.yml"
MANIFEST = Path("/srv/osiptel/evidence/manifest_bronze_da.json")

def sha256_archivo(ruta):
    digest = hashlib.sha256()
    with open(ruta, "rb") as archivo:
        for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
            digest.update(bloque)
    return digest.hexdigest()

def comando_hdfs(*argumentos):
    return [
        "docker", "compose", "-f", COMPOSE,
        "exec", "-T", "namenode",
        "hdfs", "dfs", *argumentos
    ]

def ejecutar_hdfs(*argumentos):
    subprocess.run(comando_hdfs(*argumentos), check=True)

def existe_hdfs(ruta):
    resultado = subprocess.run(
        comando_hdfs("-test", "-e", ruta),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE
    )
    if resultado.returncode not in (0, 1):
        raise RuntimeError(resultado.stderr.decode(errors="replace"))
    return resultado.returncode == 0

def sha256_hdfs(ruta):
    proceso = subprocess.Popen(
        comando_hdfs("-cat", ruta),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    digest = hashlib.sha256()
    while True:
        bloque = proceso.stdout.read(1024 * 1024)
        if not bloque:
            break
        digest.update(bloque)

    if proceso.wait() != 0:
        raise RuntimeError("Error leyendo HDFS: " + ruta)
    return digest.hexdigest()

def subir_archivo(local, destino, sha_esperado):
    ejecutar_hdfs("-mkdir", "-p", destino.rsplit("/", 1)[0])

    if existe_hdfs(destino):
        if sha256_hdfs(destino) != sha_esperado:
            raise RuntimeError(
                "Existe un archivo diferente en HDFS: " + destino
            )
        return "existente"

    with open(local, "rb") as archivo:
        subprocess.run(
            comando_hdfs("-put", "-", destino),
            stdin=archivo,
            check=True
        )

    if sha256_hdfs(destino) != sha_esperado:
        raise RuntimeError("Fallo de integridad: " + destino)

    return "cargado"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not ORIGEN.is_dir() or not ZIP.is_file():
        raise FileNotFoundError("Falta el dataset da o su ZIP original")

    version = sha256_archivo(ZIP)[:12]
    destino_base = "/data/raw/da/" + version

    manifest = {
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": "da",
        "sha256_zip": sha256_archivo(ZIP),
        "destino_base": destino_base,
        "archivos": []
    }

    totales = {}

    for numero in range(1, 5):
        fuente = "Fuente " + str(numero)
        carpeta = ORIGEN / fuente

        if not carpeta.is_dir():
            raise FileNotFoundError(str(carpeta))

        archivos = sorted(
            p for p in carpeta.rglob("*")
            if p.is_file() and p.name != ".DS_Store"
        )
        totales[fuente] = len(archivos)

        for archivo in archivos:
            relativa = archivo.relative_to(carpeta).as_posix()
            destino = (
                destino_base + "/fuente_" + str(numero) + "/" + relativa
            )
            sha = sha256_archivo(archivo)

            estado = "simulado"
            if not args.dry_run:
                estado = subir_archivo(archivo, destino, sha)

            manifest["archivos"].append({
                "fuente": fuente,
                "nombre": relativa,
                "bytes": archivo.stat().st_size,
                "sha256": sha,
                "ruta_hdfs": destino,
                "estado": estado
            })

        print(f"{fuente}: {len(archivos)} archivos")

    total = len(manifest["archivos"])
    print("TOTAL:", total)

    if total != 335:
        raise RuntimeError(
            f"Inventario inesperado: {total} archivos; se esperaban 335"
        )

    if args.dry_run:
        print("SIMULACION EXITOSA: sin cambios en HDFS")
    else:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        print("INGESTA BRONZE EXITOSA")
        print("Manifiesto:", MANIFEST)

if __name__ == "__main__":
    main()
