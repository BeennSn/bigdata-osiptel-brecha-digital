import os
import subprocess
from src.ingestion.download_raw import descargar_pdfs_desde_drive

def run_pipeline():
    print("==================================================")
    print("🚀 INICIANDO PIPELINE DE BIG DATA - OSIPTEL")
    print("==================================================")
    
    # 1. PASO: Staging Local (Descarga de PDFs desde Google Drive)
    print("\n[Paso 1/2] Ejecutando descarga de PDFs en Staging Local...")
    try:
        descargar_pdfs_desde_drive()
        print("✅ Descarga local completada en la carpeta 'data/raw'")
    except Exception as e:
        print(f"❌ Error durante la descarga local: {e}")
        return

    