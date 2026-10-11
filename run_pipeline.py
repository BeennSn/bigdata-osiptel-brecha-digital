import argparse
import logging
import sys
from src.ingestion.download_raw import download_raw_data
from src.ingestion.ingest_osiptel import ingest_osiptel_sources
from src.processing.parse_to_silver import process_general_silver
from src.processing.parse_inei_to_silver import process_inei_silver
from src.processing.parse_regulatory_to_silver import process_regulatory_silver
from src.processing.parse_conexiones_to_silver import process_conexiones_silver
from src.processing.integrate_gold import build_gold_dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main(forzar: bool = False):
    logging.info("==================================================")
    logging.info("INICIANDO PIPELINE DE BIG DATA - OSIPTEL & INEI")
    logging.info("Modo forzado: %s", "SÍ (reprocesamiento total)" if forzar else "NO (persistencia/idempotente)")
    logging.info("==================================================")

    # 1. Ingesta (Raw): Descarga de fuentes y subida al Data Lake
    logging.info("=== 1. FASE RAW / INGESTION ===")
    try:
        download_raw_data(forzar=forzar)
        ingest_osiptel_sources(forzar=forzar)
    except Exception as e:
        logging.error("Fallo durante la Fase Raw / Ingestión: %s", e)
        raise

    # 2. Procesamiento (Silver): Limpieza, tipado y estandarización
    logging.info("=== 2. FASE SILVER / LIMPIEZA Y ESTANDARIZACIÓN ===")
    try:
        process_general_silver(forzar=forzar)
        process_inei_silver(forzar=forzar)
        process_regulatory_silver(forzar=forzar)
        process_conexiones_silver(forzar=forzar)
    except Exception as e:
        logging.error("Fallo durante la Fase Silver / Procesamiento: %s", e)
        raise

    # 3. Integración (Gold): Cruce de tablas, agregaciones analíticas y persistencia
    logging.info("=== 3. FASE GOLD / MODELADO E INTEGRACIÓN ===")
    try:
        build_gold_dataset(forzar=forzar)
    except Exception as e:
        logging.error("Fallo durante la Fase Gold / Integración: %s", e)
        raise

    logging.info("==================================================")
    logging.info("✅ Pipeline completado con éxito. Capa Gold lista para consumo.")
    logging.info("==================================================")


# Alias para retrocompatibilidad
run_pipeline = main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Big Data OSIPTEL & INEI")
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Fuerza la ejecución de todos los pasos ignorando verificaciones de persistencia previa.",
    )
    args = parser.parse_args()
    main(forzar=args.force)