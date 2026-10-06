import os
import gdown

# Enlace de tu carpeta de Google Drive proporcionado
URL_DRIVE = "https://drive.google.com/drive/folders/15e9fcxAt3jdUhQacYWKi4jWrQfWH_Utd?usp=drive_link"
DIRECTORIO_DESTINO = "data/raw"

def descargar_pdfs_desde_drive():
    print("=== INICIANDO DESCARGA AUTOMÁTICA DE PDFs DESDE GOOGLE DRIVE ===")
    
    # Asegurar que la carpeta local destino exista
    os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)
    
    # Descargar la carpeta completa usando gdown
    try:
        gdown.download_folder(URL_DRIVE, output=DIRECTORIO_DESTINO, quiet=False, use_cookies=False)
        print(f"\n[ÉXITO] Todos los PDFs han sido descargados en: {DIRECTORIO_DESTINO}")
    except Exception as e:
        print(f"[ERROR] No se pudo completar la descarga desde Google Drive: {e}")

if __name__ == "__main__":
    descargar_pdfs_desde_drive()