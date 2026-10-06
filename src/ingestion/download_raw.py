import os
import zipfile
import gdown

FILE_ID = "1MGH-8byGDfNDaHqLeS629dhPIlOte18C"
URL_DIRECTA = f"https://drive.google.com/uc?id={FILE_ID}&export=download"
DIRECTORIO_DESTINO = "data/raw"
ARCHIVO_ZIP_TMP = "data/raw/temp_data.zip"

def descargar_y_descomprimir():
    print("=== INICIANDO DESCARGA Y DESCOMPRESIÓN DEL ZIP ===")
    os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)
    
    try:
        print("Descargando archivo comprimido...")
        # Se añade confirm_large=True para saltar el aviso de virus de Google Drive
        gdown.download(URL_DIRECTA, ARCHIVO_ZIP_TMP, quiet=False, fuzzy=True, confirm_large=True)
        
        print(f"Descomprimiendo archivos en {DIRECTORIO_DESTINO}...")
        with zipfile.ZipFile(ARCHIVO_ZIP_TMP, 'r') as zip_ref:
            zip_ref.extractall(DIRECTORIO_DESTINO)
            
        os.remove(ARCHIVO_ZIP_TMP)
        print(f"\n[ÉXITO] Archivos descargados y descomprimidos en: {DIRECTORIO_DESTINO}")
        
    except Exception as e:
        print(f"[ERROR] Ocurrió un fallo durante el proceso: {e}")

if __name__ == "__main__":
    descargar_y_descomprimir()