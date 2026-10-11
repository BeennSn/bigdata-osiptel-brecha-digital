import os
import glob
import shutil
import zipfile

FILE_ID = "1MGH-8byGDfNDaHqLeS629dhPIlOte18C"
URL_DIRECTA = f"https://drive.google.com/uc?id={FILE_ID}&export=download"
DIRECTORIO_DESTINO = "data/raw"
DIRECTORIO_DATOS = "data/raw/datos"
ARCHIVO_ZIP_TMP = "data/raw/temp_data.zip"


def _hay_datos_locales() -> tuple:
    """Verifica si los 4 datasets ya están disponibles en data/raw/datos o en data/raw."""
    # 1. Verificar en data/raw/datos/
    if os.path.exists(DIRECTORIO_DATOS):
        xlsxs = glob.glob(os.path.join(DIRECTORIO_DATOS, "**", "*.xlsx"), recursive=True)
        pdfs = glob.glob(os.path.join(DIRECTORIO_DATOS, "**", "*.pdf"), recursive=True)
        if xlsxs and pdfs:
            return True, f"{DIRECTORIO_DATOS}/ ({len(pdfs)} PDFs y {len(xlsxs)} XLSX organizados en subcarpetas)"

    # 2. Verificar en data/raw/ raíz
    if os.path.exists(DIRECTORIO_DESTINO):
        xlsxs_raiz = glob.glob(os.path.join(DIRECTORIO_DESTINO, "*.xlsx"))
        pdfs_raiz = glob.glob(os.path.join(DIRECTORIO_DESTINO, "*.pdf"))
        if xlsxs_raiz and pdfs_raiz:
            return True, f"{DIRECTORIO_DESTINO}/ ({len(pdfs_raiz)} PDFs y {len(xlsxs_raiz)} XLSX)"

    return False, ""


def descargar_y_descomprimir(forzar: bool = False):
    print("=== INICIANDO VERIFICACIÓN Y DESCARGA EN STAGING LOCAL ===")
    os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)

    # Verificación de persistencia previa
    if not forzar:
        existe, detalle = _hay_datos_locales()
        if existe:
            print(f" [INFO] Datasets locales detectados en: {detalle}")
            print(" [INFO] Se omite la descarga por persistencia. Usa forzar=True si requieres re-descargar.")
            return

    try:
        import gdown
        print("Descargando archivo comprimido desde Google Drive...")
        gdown.download(URL_DIRECTA, ARCHIVO_ZIP_TMP, quiet=False)

        print("Descomprimiendo archivos en data/raw/...")
        with zipfile.ZipFile(ARCHIVO_ZIP_TMP, "r") as zip_ref:
            zip_ref.extractall(DIRECTORIO_DESTINO)

        if os.path.exists(ARCHIVO_ZIP_TMP):
            os.remove(ARCHIVO_ZIP_TMP)

        # Limpieza de archivos ocultos / metadata de macOS
        for root, dirs, files in os.walk(DIRECTORIO_DESTINO, topdown=False):
            for d in dirs:
                if d in ["__MACOSX", ".DS_Store"]:
                    shutil.rmtree(os.path.join(root, d), ignore_errors=True)
            for f in files:
                if f.startswith("._") or f == ".DS_Store":
                    try:
                        os.remove(os.path.join(root, f))
                    except OSError:
                        pass

        # Si se extrajo una carpeta intermedia o raíz de datos, asegurar data/raw/datos
        if not os.path.exists(DIRECTORIO_DATOS):
            carpetas_hijas = [
                d for d in os.listdir(DIRECTORIO_DESTINO)
                if os.path.isdir(os.path.join(DIRECTORIO_DESTINO, d)) and d != "datos"
            ]
            if any(c in ["DatasetINEI", "Osiptal_DatosNE", "ReportesOsiptel", "internet_fijo"] for c in [c.strip() for c in carpetas_hijas]):
                os.makedirs(DIRECTORIO_DATOS, exist_ok=True)
                for d in carpetas_hijas:
                    origen = os.path.join(DIRECTORIO_DESTINO, d)
                    destino = os.path.join(DIRECTORIO_DATOS, d.strip())
                    if origen != destino and os.path.exists(origen):
                        shutil.move(origen, destino)

        print(f"\n[ÉXITO] Archivos listos en staging local ({DIRECTORIO_DESTINO}/)")

    except Exception as e:
        print(f"[ERROR] Ocurrió un fallo durante la descarga/descompresión: {e}")
        raise


def download_raw_data(forzar: bool = False):
    """Punto de entrada estándar para descarga en staging local."""
    return descargar_y_descomprimir(forzar=forzar)


if __name__ == "__main__":
    download_raw_data()