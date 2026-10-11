import argparse
import sys
from src.ingestion.download_raw import descargar_y_descomprimir as descargar_fuentes_desde_drive
from src.ingestion.ingest_osiptel import ejecutar_ingesta_hdfs
from src.processing.parse_to_silver import procesar_a_silver_con_yarn
from src.processing.parse_conexiones_to_silver import procesar_conexiones_a_silver
from src.processing.integrate_gold import procesar_a_gold_con_yarn


def run_pipeline(forzar: bool = False):
    print("==================================================")
    print("INICIANDO PIPELINE DE BIG DATA - OSIPTEL & INEI")
    print(f"Modo forzado: {'SÍ (reprocesamiento total)' if forzar else 'NO (persistencia/idempotente)'}")
    print("==================================================")

    # 1. Staging local (descarga de PDF y XLSX desde Google Drive)
    print("\n[Paso 1/5] Ejecutando descarga en Staging Local...")
    try:
        descargar_fuentes_desde_drive(forzar=forzar)
    except Exception as e:
        print(f"Error en la descarga local: {e}")
        raise

    # 2. Ingesta al Data Lake (HDFS Raw + Manifest)
    print("\n[Paso 2/5] Ejecutando ingesta a HDFS y generación de Manifest...")
    try:
        ejecutar_ingesta_hdfs(forzar=forzar)
    except Exception as e:
        print(f"Error en la ingesta a HDFS: {e}")
        raise

    # 3. Procesamiento con Spark y YARN (capa Silver de PDFs)
    print("\n[Paso 3/5] Ejecutando procesamiento Spark/YARN a Capa Silver de PDFs...")
    try:
        procesar_a_silver_con_yarn(forzar=forzar)
    except Exception as e:
        print(f"Error en el procesamiento a Silver de PDFs: {e}")
        raise

    # 4. Procesamiento con Spark y YARN (Capa Silver de conexiones internet fijo)
    print("\n[Paso 4/5] Ejecutando procesamiento Spark/YARN a Capa Silver de conexiones internet fijo...")
    try:
        procesar_conexiones_a_silver(forzar=forzar)
    except Exception as e:
        print(f"Error en el procesamiento a Silver de conexiones: {e}")
        raise

    # 5. Integración analítica con Spark y YARN (Capa Gold consolidada)
    print("\n[Paso 5/5] Ejecutando integración analítica a Capa Gold en HDFS...")
    try:
        procesar_a_gold_con_yarn(forzar=forzar)
    except Exception as e:
        print(f"Error en la integración a Gold: {e}")
        raise

    print("\n==================================================")
    print("Pipeline finalizado exitosamente: Raw -> Silver -> Gold")
    print("==================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Big Data OSIPTEL")
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Fuerza la ejecución de todos los pasos ignorando verificaciones de persistencia previa.",
    )
    args = parser.parse_args()
    run_pipeline(forzar=args.force)