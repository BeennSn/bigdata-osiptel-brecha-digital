import json
from pyspark.sql import SparkSession

RUTA = (
    "hdfs://namenode:8020/data/gold/"
    "osiptel_alertas_revision_f1/"
    "v1_943cfa06696d30e9dca9"
)

spark = (
    SparkSession.builder
    .appName("OSIPTEL_Matriz_Decisiones_Gold")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(RUTA)

    columnas = [
        "operador",
        "departamento",
        "conexiones_base",
        "conexiones_actual",
        "delta_conexiones",
        "variacion_porcentual",
        "prioridad",
        "estado"
    ]

    registros = [r.asDict() for r in df.select(*columnas).collect()]

    if len(registros) != 30:
        raise RuntimeError("Se esperaban 30 alertas Gold")

    altas = sum(r["prioridad"] == "ALTA" for r in registros)
    medias = sum(r["prioridad"] == "MEDIA" for r in registros)

    if (altas, medias) != (11, 19):
        raise RuntimeError("Distribucion de prioridades inesperada")

    if len({(r["operador"], r["departamento"]) for r in registros}) != 30:
        raise RuntimeError("Existen combinaciones duplicadas")

    registros.sort(
        key=lambda r: (
            0 if r["prioridad"] == "ALTA" else 1,
            -abs(r["delta_conexiones"]),
            r["operador"],
            r["departamento"]
        )
    )

    matriz = []

    for i, r in enumerate(registros, 1):
        prioridad = r["prioridad"]

        matriz.append({
            "orden_revision": i,
            "prioridad": prioridad,
            "operador": r["operador"],
            "departamento": r["departamento"],
            "periodo_base": "2025-12",
            "periodo_actual": "2026-03",
            "conexiones_base": r["conexiones_base"],
            "conexiones_actual": r["conexiones_actual"],
            "delta_conexiones": r["delta_conexiones"],
            "variacion_porcentual": r["variacion_porcentual"],
            "tipo_variacion": (
                "DISMINUCION" if r["delta_conexiones"] < 0
                else "AUMENTO"
            ),
            "decision_propuesta": (
                "REVISION PRIORITARIA DEL REPORTE"
                if prioridad == "ALTA"
                else "REVISION SECUNDARIA DEL REPORTE"
            ),
            "accion": (
                "Contrastar cifras con reportes del operador, "
                "periodos de declaracion y posibles cambios metodologicos"
            ),
            "estado": r["estado"]
        })

    print("=== MATRIZ_REVISION_GOLD_JSON ===")
    print(json.dumps({
        "fuente": "OSIPTEL Fuente 1",
        "total_alertas": len(matriz),
        "prioridad_alta": altas,
        "prioridad_media": medias,
        "matriz": matriz
    }, indent=2, ensure_ascii=False))

finally:
    spark.stop()
