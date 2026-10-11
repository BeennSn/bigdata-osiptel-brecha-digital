import os
import json
import glob
import datetime
import subprocess

MANIFEST_PATH = "data/manifest.json"
HDFS_TARGET_DIR = "/data/raw/osiptel_pdfs/"
HDFS_XLSX_DIR = "/data/raw/conexiones_internet_fijo/"
LOCAL_RAW_DIR = "data/raw"

def subir_a_hdfs():
    """Sube los PDFs y el XLSX locales de la capa Staging hacia el Data Lake en HDFS."""
    print(" [INGESTA] Iniciando transferencia de archivos hacia HDFS...")
    
    if not os.path.exists(LOCAL_RAW_DIR) or not os.listdir(LOCAL_RAW_DIR):
        raise FileNotFoundError(f" No se encontraron archivos en {LOCAL_RAW_DIR} para subir a HDFS.")

    try:
        # 1. Crear carpeta en HDFS
        subprocess.run(f"hdfs dfs -mkdir -p {HDFS_TARGET_DIR}", shell=True, check=True)
        
        # 2. Subir PDFs con -put (forzando sobrescritura si es necesario)
        subprocess.run(f"hdfs dfs -put -f {LOCAL_RAW_DIR}/*.pdf {HDFS_TARGET_DIR}", shell=True, check=True)
        print(f" [ÉXITO] PDFs subidos correctamente a HDFS: {HDFS_TARGET_DIR}")

        # 3. Subir el XLSX (fuente estructurada) a su propia carpeta
        if glob.glob(f"{LOCAL_RAW_DIR}/*.xlsx"):
            subprocess.run(f"hdfs dfs -mkdir -p {HDFS_XLSX_DIR}", shell=True, check=True)
            subprocess.run(f"hdfs dfs -put -f {LOCAL_RAW_DIR}/*.xlsx {HDFS_XLSX_DIR}", shell=True, check=True)
            print(f" [ÉXITO] XLSX subido correctamente a HDFS: {HDFS_XLSX_DIR}")
        else:
            print(" [ADVERTENCIA] No hay .xlsx en data/raw; se omite la fuente estructurada.")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Falló la interacción con HDFS: {e}")
        print("💡 Consejo: Verifica que los servicios de Hadoop estén activos en tu máquina virtual.")
        raise

def generar_manifest():
    """Genera un archivo de manifiesto (manifest.json) con el registro de la ingesta."""
    print("📋 [MANIFIESTO] Registrando metadatos de los archivos ingeridos...")
    
    os.makedirs("data", exist_ok=True)
    
    archivos = [f for f in os.listdir(LOCAL_RAW_DIR) if f.endswith('.pdf')] if os.path.exists(LOCAL_RAW_DIR) else []
    archivos_xlsx = [f for f in os.listdir(LOCAL_RAW_DIR) if f.endswith('.xlsx')] if os.path.exists(LOCAL_RAW_DIR) else []
    
    manifest_data = {
        "timestamp_ingesta": datetime.datetime.now().isoformat(),
        "capa_origen": "staging_local",
        "capa_destino": "hdfs_raw",
        "ruta_hdfs": HDFS_TARGET_DIR,
        "total_archivos": len(archivos),
        "archivos": archivos,
        "ruta_hdfs_xlsx": HDFS_XLSX_DIR,
        "total_archivos_xlsx": len(archivos_xlsx),
        "archivos_xlsx": archivos_xlsx
    }
    
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=4, ensure_ascii=False)
        
    print(f"[ÉXITO] Manifest guardado en: {MANIFEST_PATH}")

def ejecutar_ingesta_hdfs():
    """Función principal llamada por el orquestador."""
    subir_a_hdfs()
    generar_manifest()

if __name__ == "__main__":
    ejecutar_ingesta_hdfs()