import datetime
import glob
import json
import os
import subprocess

MANIFEST_PATH = "data/manifest.json"
BASE_LOCAL_DATOS = "data/raw/datos" if os.path.exists("data/raw/datos") else "data/raw"

# Mapeo de datasets a sus carpetas destino en HDFS
HDFS_RUTAS = {
    "osiptel_mandatos": "/data/raw/osiptel_pdfs/",
    "internet_fijo": "/data/raw/conexiones_internet_fijo/",
    "dataset_inei": "/data/raw/dataset_inei/",
    "reportes_osiptel": "/data/raw/reportes_osiptel/",
}


def _resolver_carpeta_local(nombre_clave: str) -> str:
    """Busca una subcarpeta en BASE_LOCAL_DATOS ignorando espacios en blanco y mayúsculas."""
    if not os.path.exists(BASE_LOCAL_DATOS):
        return ""
    for item in os.listdir(BASE_LOCAL_DATOS):
        if item.strip().lower() == nombre_clave.lower():
            return os.path.join(BASE_LOCAL_DATOS, item)
    return ""


def _obtener_archivos_dataset(nombre_clave: str, extension: str) -> list:
    """Obtiene la lista de archivos de un dataset específico."""
    carpeta = _resolver_carpeta_local(nombre_clave)
    if carpeta and os.path.isdir(carpeta):
        archivos = glob.glob(os.path.join(carpeta, f"*.{extension}"))
        if archivos:
            return sorted(archivos)

    # Fallback: buscar directamente en data/raw/ o data/raw/datos/
    return sorted(glob.glob(os.path.join(BASE_LOCAL_DATOS, f"*.{extension}")))


def _hdfs_tiene_archivos(directorio: str) -> bool:
    """Verifica si un directorio de HDFS existe y contiene al menos un archivo."""
    try:
        res = subprocess.run(
            ["hdfs", "dfs", "-ls", directorio],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0:
            lineas = [l for l in res.stdout.strip().splitlines() if not l.startswith("Found")]
            return len(lineas) > 0
    except Exception:
        pass
    return False


def subir_a_hdfs(forzar: bool = False):
    """Sube los 4 datasets locales (3 no estructurados + 1 estructurado) a HDFS."""
    print("=== [INGESTA] INICIANDO TRANSFERENCIA DE 4 DATASETS A HDFS ===")
    print(f"Directorio local de origen: {BASE_LOCAL_DATOS}/")

    # 1. Definición de los 4 datasets
    datasets_info = {
        "osiptel_mandatos": {
            "nombre_local": "Osiptal_DatosNE",
            "extension": "pdf",
            "hdfs_dir": HDFS_RUTAS["osiptel_mandatos"],
            "descripcion": "Mandatos de interconexión OSIPTEL (no estructurado)",
        },
        "internet_fijo": {
            "nombre_local": "internet_fijo",
            "extension": "xlsx",
            "hdfs_dir": HDFS_RUTAS["internet_fijo"],
            "descripcion": "Conexiones internet fijo (estructurado)",
        },
        "dataset_inei": {
            "nombre_local": "DatasetINEI",
            "extension": "pdf",
            "hdfs_dir": HDFS_RUTAS["dataset_inei"],
            "descripcion": "Informes técnicos INEI (no estructurado)",
        },
        "reportes_osiptel": {
            "nombre_local": "ReportesOsiptel",
            "extension": "pdf",
            "hdfs_dir": HDFS_RUTAS["reportes_osiptel"],
            "descripcion": "Reportes estadísticos OSIPTEL (no estructurado)",
        },
    }

    try:
        for clave, meta in datasets_info.items():
            hdfs_dir = meta["hdfs_dir"]
            archivos = _obtener_archivos_dataset(meta["nombre_local"], meta["extension"])

            print(f"\n--- Procesando {meta['descripcion']} ---")
            print(f"Archivos encontrados localmente: {len(archivos)}")

            if not archivos:
                print(f" [ADVERTENCIA] No se encontraron archivos para {clave} en {BASE_LOCAL_DATOS}.")
                continue

            # Verificación de persistencia previa en HDFS
            if not forzar and _hdfs_tiene_archivos(hdfs_dir):
                print(f" [INFO] El directorio HDFS {hdfs_dir} ya contiene archivos.")
                print(" [INFO] Se omite la subida por persistencia (usa forzar=True si requieres sobreescribir).")
            else:
                # Crear carpeta en HDFS
                subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)

                # Subida en lotes para no exceder longitud máxima de comando
                chunk_size = 50
                for i in range(0, len(archivos), chunk_size):
                    chunk = archivos[i : i + chunk_size]
                    for archivo_item in chunk:
                        if " " in os.path.basename(archivo_item):
                            dest_item = f"{hdfs_dir.rstrip('/')}/{os.path.basename(archivo_item)}"
                            with open(archivo_item, "rb") as f_in:
                                subprocess.run(["hdfs", "dfs", "-put", "-f", "-", dest_item], input=f_in.read(), check=True)
                        else:
                            subprocess.run(["hdfs", "dfs", "-put", "-f", archivo_item, hdfs_dir], check=True)

                print(f" [ÉXITO] {len(archivos)} archivos subidos correctamente a HDFS: {hdfs_dir}")

            # En el caso de internet fijo (XLSX), aseguramos también presencia en /data/raw/osiptel_pdfs/
            # por compatibilidad con el pipeline que busca ambos tipos de archivo en la misma carpeta
            if clave == "internet_fijo":
                hdfs_compartido = HDFS_RUTAS["osiptel_mandatos"]
                subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_compartido], check=True)
                for f_xlsx in archivos:
                    if " " in os.path.basename(f_xlsx):
                        dest_comp = f"{hdfs_compartido.rstrip('/')}/{os.path.basename(f_xlsx)}"
                        with open(f_xlsx, "rb") as f_in:
                            subprocess.run(["hdfs", "dfs", "-put", "-f", "-", dest_comp], input=f_in.read(), check=True)
                    else:
                        subprocess.run(["hdfs", "dfs", "-put", "-f", f_xlsx, hdfs_compartido], check=True)
                print(f" [ÉXITO] XLSX replicado en ruta compartida: {hdfs_compartido}")


    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Falló la interacción con HDFS durante la ingesta: {e}")
        print("💡 Consejo: Verifica que los servicios de Hadoop estén activos en tu VM (jps: NameNode, DataNode).")
        raise


def generar_manifest():
    """Genera manifest.json detallando los 4 datasets y garantizando retrocompatibilidad."""
    print("\n📋 [MANIFIESTO] Registrando metadatos de los 4 datasets ingeridos...")
    os.makedirs("data", exist_ok=True)

    # Identificar archivos de cada uno de los 4 datasets
    mandatos = [os.path.basename(f) for f in _obtener_archivos_dataset("Osiptal_DatosNE", "pdf")]
    xlsx_fijo = [os.path.basename(f) for f in _obtener_archivos_dataset("internet_fijo", "xlsx")]
    inei = [os.path.basename(f) for f in _obtener_archivos_dataset("DatasetINEI", "pdf")]
    reportes = [os.path.basename(f) for f in _obtener_archivos_dataset("ReportesOsiptel", "pdf")]

    manifest_data = {
        "timestamp_ingesta": datetime.datetime.now().isoformat(),
        "capa_origen": f"staging_local ({BASE_LOCAL_DATOS})",
        "capa_destino": "hdfs_raw (/data/raw/)",
        "total_datasets": 4,
        "datasets": {
            "internet_fijo": {
                "tipo": "estructurado",
                "ruta_local": _resolver_carpeta_local("internet_fijo") or BASE_LOCAL_DATOS,
                "ruta_hdfs": HDFS_RUTAS["internet_fijo"],
                "total_archivos": len(xlsx_fijo),
                "archivos": xlsx_fijo,
            },
            "osiptel_mandatos": {
                "tipo": "no_estructurado",
                "ruta_local": _resolver_carpeta_local("Osiptal_DatosNE") or BASE_LOCAL_DATOS,
                "ruta_hdfs": HDFS_RUTAS["osiptel_mandatos"],
                "total_archivos": len(mandatos),
                "archivos": mandatos,
            },
            "dataset_inei": {
                "tipo": "no_estructurado",
                "ruta_local": _resolver_carpeta_local("DatasetINEI") or BASE_LOCAL_DATOS,
                "ruta_hdfs": HDFS_RUTAS["dataset_inei"],
                "total_archivos": len(inei),
                "archivos": inei,
            },
            "reportes_osiptel": {
                "tipo": "no_estructurado",
                "ruta_local": _resolver_carpeta_local("ReportesOsiptel") or BASE_LOCAL_DATOS,
                "ruta_hdfs": HDFS_RUTAS["reportes_osiptel"],
                "total_archivos": len(reportes),
                "archivos": reportes,
            },
        },
        # Claves de retrocompatibilidad directa requeridas por etapas Silver
        "ruta_hdfs": HDFS_RUTAS["osiptel_mandatos"],
        "total_archivos": len(mandatos),
        "archivos": mandatos,
        "ruta_hdfs_xlsx": HDFS_RUTAS["internet_fijo"],
        "total_archivos_xlsx": len(xlsx_fijo),
        "archivos_xlsx": xlsx_fijo,
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=4, ensure_ascii=False)

    print(f"[ÉXITO] Manifiesto con 4 datasets guardado en: {MANIFEST_PATH}")
    print(f" - Internet Fijo (XLSX): {len(xlsx_fijo)} archivos")
    print(f" - Mandatos OSIPTEL (PDF): {len(mandatos)} archivos")
    print(f" - Informes INEI (PDF): {len(inei)} archivos")
    print(f" - Reportes Estadísticos OSIPTEL (PDF): {len(reportes)} archivos")


def ejecutar_ingesta_hdfs(forzar: bool = False):
    """Función principal llamada por el orquestador run_pipeline.py."""
    subir_a_hdfs(forzar=forzar)
    if not os.path.exists(MANIFEST_PATH) or forzar:
        generar_manifest()
    else:
        print(f"[INFO] Manifiesto existente en {MANIFEST_PATH}. Conservado por persistencia.")


def ingest_osiptel_sources(forzar: bool = False):
    """Punto de entrada estándar para ingesta a HDFS y generación de manifiesto."""
    return ejecutar_ingesta_hdfs(forzar=forzar)



if __name__ == "__main__":
    ingest_osiptel_sources()
