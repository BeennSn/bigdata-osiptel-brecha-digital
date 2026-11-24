from pyspark.sql import SparkSession
import json

BASE = "hdfs://namenode:8020/data/gold/"
VERSION = "v1_943cfa06696d30e9dca9"

spark = (
    SparkSession.builder
    .appName("OSIPTEL_Verificacion_Alertas_Publicadas")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    panel = spark.read.parquet(
        BASE + "osiptel_variacion_operador_departamento_f1/" + VERSION
    ).collect()

    publicadas = spark.read.parquet(
        BASE + "osiptel_alertas_revision_f1/" + VERSION
    ).collect()

    elegibles = [
        r for r in panel
        if r["operador"] not in (None, "SIN_OPERADOR")
        and r["departamento"] not in (None, "SIN_DEPARTAMENTO")
        and r["conexiones_base"] is not None
        and r["conexiones_base"] >= 1000
        and r["delta_conexiones"] is not None
        and r["variacion_porcentual"] is not None
    ]

    clave = lambda r: (r["operador"], r["departamento"])

    esperadas = {}
    for r in elegibles:
        if abs(r["delta_conexiones"]) < 1000:
            continue
        if abs(r["variacion_porcentual"]) < 15:
            continue

        alta = (
            abs(r["delta_conexiones"]) >= 10000
            and abs(r["variacion_porcentual"]) >= 20
        )
        esperadas[clave(r)] = "ALTA" if alta else "MEDIA"

    reales = {clave(r): r["prioridad"] for r in publicadas}

    if len(reales) != len(publicadas):
        raise RuntimeError("Existen claves duplicadas en Gold")

    faltantes = sorted(set(esperadas) - set(reales))
    sobrantes = sorted(set(reales) - set(esperadas))
    prioridad_distinta = [
        k for k in esperadas.keys() & reales.keys()
        if esperadas[k] != reales[k]
    ]

    referencia = sorted(
        elegibles,
        key=lambda r: (
            -abs(r["delta_conexiones"]),
            r["operador"],
            r["departamento"]
        )
    )[:30]

    claves_ref = {clave(r) for r in referencia}
    claves_regla = set(esperadas)
    por_clave = {clave(r): r for r in elegibles}

    def detalle(k):
        r = por_clave[k]
        return {
            "operador": k[0],
            "departamento": k[1],
            "delta": int(r["delta_conexiones"]),
            "variacion_pct": float(r["variacion_porcentual"])
        }

    resultado = {
        "panel": len(panel),
        "elegibles": len(elegibles),
        "alertas_recalculadas": len(esperadas),
        "alertas_publicadas": len(publicadas),
        "faltantes": faltantes,
        "sobrantes": sobrantes,
        "prioridades_distintas": prioridad_distinta,
        "coincidencias_top30": len(claves_regla & claves_ref),
        "solo_nuestra_regla": [
            detalle(k) for k in sorted(claves_regla - claves_ref)
        ],
        "solo_referencia": [
            detalle(k) for k in sorted(claves_ref - claves_regla)
        ]
    }

    print("=== VERIFICACION GOLD ===")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))

    if faltantes or sobrantes or prioridad_distinta:
        raise RuntimeError("Gold no coincide con las reglas")

finally:
    spark.stop()
