from pyspark.sql import SparkSession
from collections import Counter
import json

RUTA = (
    "hdfs://namenode:8020/data/gold/"
    "osiptel_variacion_operador_departamento_f1/"
    "v1_943cfa06696d30e9dca9"
)

spark = (
    SparkSession.builder
    .appName("OSIPTEL_Evaluacion_Alertas_Gold")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(RUTA)
    filas_panel = df.count()

    columnas = [
        "operador", "departamento", "conexiones_base",
        "delta_conexiones", "variacion_porcentual"
    ]

    filas = []

    for r in df.select(*columnas).collect():
        if any(r[c] is None for c in columnas):
            continue

        filas.append({
            "operador": str(r["operador"]),
            "departamento": str(r["departamento"]),
            "base": int(r["conexiones_base"]),
            "delta": int(r["delta_conexiones"]),
            "pct": float(r["variacion_porcentual"])
        })

    elegibles = [
        r for r in filas
        if r["operador"] != "SIN_OPERADOR"
        and r["departamento"] != "SIN_DEPARTAMENTO"
        and r["base"] >= 1000
    ]

    def seleccionar(min_abs, min_pct):
        return [
            r for r in elegibles
            if abs(r["delta"]) >= min_abs
            and abs(r["pct"]) >= min_pct
        ]

    def clave(r):
        return (r["operador"], r["departamento"])

    actual = seleccionar(1000, 15)

    referencia = sorted(
        elegibles,
        key=lambda r: (
            -abs(r["delta"]),
            r["operador"],
            r["departamento"]
        )
    )[:30]

    claves_actual = {clave(r) for r in actual}
    claves_referencia = {clave(r) for r in referencia}

    escenarios = []

    for minimo_abs, minimo_pct in [
        (500, 10),
        (1000, 15),
        (2000, 20),
        (5000, 25)
    ]:
        seleccion = seleccionar(minimo_abs, minimo_pct)

        escenarios.append({
            "minimo_conexiones": minimo_abs,
            "minimo_porcentaje": minimo_pct,
            "alertas": len(seleccion),
            "aumentos": sum(r["delta"] > 0 for r in seleccion),
            "disminuciones": sum(r["delta"] < 0 for r in seleccion)
        })

    altas = [
        r for r in actual
        if abs(r["delta"]) >= 10000
        and abs(r["pct"]) >= 20
    ]

    resultado = {
        "filas_panel": filas_panel,
        "filas_con_campos_completos": len(filas),
        "filas_excluidas_por_nulos": filas_panel - len(filas),
        "combinaciones_elegibles": len(elegibles),
        "regla_actual": {
            "alertas": len(actual),
            "prioridad_alta": len(altas),
            "prioridad_media": len(actual) - len(altas),
            "aumentos": sum(r["delta"] > 0 for r in actual),
            "disminuciones": sum(r["delta"] < 0 for r in actual)
        },
        "sensibilidad_umbral": escenarios,
        "comparacion_top30_variacion_absoluta": {
            "coincidencias": len(
                claves_actual & claves_referencia
            ),
            "solo_regla_actual": len(
                claves_actual - claves_referencia
            ),
            "solo_referencia": len(
                claves_referencia - claves_actual
            )
        }
    }

    if filas_panel != 611 or len(actual) != 30:
        print(json.dumps(resultado, indent=2, ensure_ascii=False))
        raise RuntimeError(
            "El panel o las alertas no coinciden con Gold."
        )

    print("\n=== EVALUACION ALERTAS GOLD ===")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))

finally:
    spark.stop()
