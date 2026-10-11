from src.ingestion.download_raw import descargar_y_descomprimir as descargar_fuentes_desde_drive
from src.ingestion.ingest_osiptel import ejecutar_ingesta_hdfs
from src.processing.parse_to_silver import procesar_a_silver_con_yarn


def run_pipeline():
    print("==================================================")
    print("INICIANDO PIPELINE DE BIG DATA - OSIPTEL")
    print("==================================================")

    # 1. Staging local (descarga de PDF y XLSX desde Google Drive)
    print("\n[Paso 1/3] Ejecutando descarga en Staging Local...")
    try:
        descargar_fuentes_desde_drive()
    except Exception as e:
        print(f"Error en la descarga local: {e}")
        return

    # 2. Ingesta al Data Lake (HDFS Raw + Manifest)
    print("\n[Paso 2/3] Ejecutando ingesta a HDFS y generación de Manifest...")
    try:
        ejecutar_ingesta_hdfs()
    except Exception as e:
        print(f"Error en la ingesta a HDFS: {e}")
        return

    # 3. Procesamiento con Spark y YARN (capa Silver)
    print("\n[Paso 3/3] Ejecutando procesamiento Spark/YARN a Capa Silver...")
    try:
        procesar_a_silver_con_yarn()
    except Exception as e:
        print(f"Error en el procesamiento a Silver: {e}")
        return

    print("\n==================================================")
    print("Pipeline ejecutado: Raw + Silver (documentos PDF)")
    print("==================================================")


if __name__ == "__main__":
    run_pipeline()