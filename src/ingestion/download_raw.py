import os
import zipfile
import gdown

# ATENCIÓN: Debes reemplazar este enlace por el enlace directo de descarga del archivo .zip en Google Drive
# (Ejemplo: https://drive.google.com/uc?id=TU_ID_DE_ARCHIVO_ZIP&export=download)
URL_ZIP_DRIVE = "https://drive.google.com/file/d/1MGH-8byGDfNDaHqLeS629dhPIlOte18C/view?usp=drive_link"
DIRECTORIO_DESTINO = "data/raw"
ARCHIVO_ZIP_TMP = "data/raw/temp_data.zip"

def descargar_y_descomprimir():
    print("=== INICIANDO DESCARGA Y DESCOMPRESIÓN AUTOMÁTICA ===")
    
    # 1. Asegurar que la carpeta local destino exista
    os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)
    
    try:
        # 2. Descargar el archivo ZIP usando gdown
        print(f"Descargando archivo comprimido...")
        gdown.download(URL_ZIP_DRIVE, ARCHIVO_ZIP_TMP, quiet=False, fuzzy=True)
        
        # 3. Descomprimir el archivo en data/raw
        print(f"Descomprimiendo archivos en {DIRECTORIO_DESTINO}...")
        with zipfile.ZipFile(ARCHIVO_ZIP_TMP, 'r') as zip_ref:
            zip_ref.extractall(DIRECTORIO_DESTINO)
            
        # 4. Limpiar el archivo ZIP temporal
        os.remove(ARCHIVO_ZIP_TMP)
        print(f"\n[ÉXITO] Archivos descargados y descomprimidos correctamente en: {DIRECTORIO_DESTINO}")
        
    except Exception as e:
        print(f"[ERROR] Ocurrió un fallo durante el proceso: {e}")

if __name__ == "__main__":
    descargar_y_descomprimir()