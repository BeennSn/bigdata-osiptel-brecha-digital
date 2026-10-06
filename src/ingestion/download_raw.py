import os
import shutil
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
        gdown.download(URL_DIRECTA, ARCHIVO_ZIP_TMP, quiet=False)
        
        print("Descomprimiendo archivos...")
        with zipfile.ZipFile(ARCHIVO_ZIP_TMP, 'r') as zip_ref:
            zip_ref.extractall(DIRECTORIO_DESTINO)
            
        os.remove(ARCHIVO_ZIP_TMP)
        
        print("Organizando archivos en data/raw/...")
        for item in os.listdir(DIRECTORIO_DESTINO):
            item_path = os.path.join(DIRECTORIO_DESTINO, item)
            
            if os.path.isdir(item_path) and item not in ["__MACOSX"]:
                for subitem in os.listdir(item_path):
                    shutil.move(os.path.join(item_path, subitem), DIRECTORIO_DESTINO)
                os.rmdir(item_path)
                
        macos_dir = os.path.join(DIRECTORIO_DESTINO, "__MACOSX")
        if os.path.exists(macos_dir):
            shutil.rmtree(macos_dir)
            
        print(f"\n[ÉXITO] Todos los PDFs quedaron listos directamente en: {DIRECTORIO_DESTINO}/")
        
    except Exception as e:
        print(f"[ERROR] Ocurrió un fallo durante el proceso: {e}")

if __name__ == "__main__":
    descargar_y_descomprimir()
