
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel


ID_MODELO = "RF_A_operativo_c5cc727_v1"
FIN_ENTRENAMIENTO = "2026-03"

FEATURES = [
    "conexiones",
    "crecimiento_trimestral",
    "crecimiento_interanual"
]


def preparar_variables(territorial):

    panel = (
        territorial
        .filter(F.col("periodo") >= "2016-03")
        .filter(F.substring("periodo", 6, 2).isin(
            "03", "06", "09", "12"
        ))
        .groupBy("departamento", "periodo")
        .agg(
            F.sum("conexiones_fijas").alias("conexiones")
        )
    )

    ventana = Window.partitionBy(
        "departamento"
    ).orderBy("periodo")

    datos = (
        panel
        .withColumn(
            "conexiones_anterior",
            F.lag("conexiones", 1).over(ventana)
        )
        .withColumn(
            "conexiones_anio_anterior",
            F.lag("conexiones", 4).over(ventana)
        )
        .withColumn(
            "periodo_anterior",
            F.lag("periodo", 1).over(ventana)
        )
        .withColumn(
            "periodo_anio_anterior",
            F.lag("periodo", 4).over(ventana)
        )
        .withColumn(
            "crecimiento_trimestral",
            F.col("conexiones") - F.col("conexiones_anterior")
        )
        .withColumn(
            "crecimiento_interanual",
            F.col("conexiones") - F.col("conexiones_anio_anterior")
        )
    )

    ultimo = panel.agg(F.max("periodo")).first()[0]

    if ultimo is None or ultimo < FIN_ENTRENAMIENTO:
        raise ValueError("Corte incompatible con el modelo operativo")

    fecha = F.to_date(
        F.concat(F.col("periodo"), F.lit("-01"))
    )

    datos = (
        datos.filter(F.col("periodo") == ultimo)
        .filter(
            F.col("periodo_anterior") ==
            F.date_format(F.add_months(fecha, -3), "yyyy-MM")
        )
        .filter(
            F.col("periodo_anio_anterior") ==
            F.date_format(F.add_months(fecha, -12), "yyyy-MM")
        )
        .dropna(subset=FEATURES)
        .withColumn(
            "periodo_prediccion",
            F.date_format(F.add_months(fecha, 3), "yyyy-MM")
        )
    )

    if datos.count() != 25:
        raise ValueError("No existen 25 territorios con historial completo")

    return datos


def ejecutar_inferencia(spark, ruta_gold, ruta_modelo):

    print("[1/4] Leyendo Gold territorial", flush=True)

    territorial = spark.read.parquet(ruta_gold)

    print("[2/4] Preparando variables historicas", flush=True)

    datos = preparar_variables(territorial)

    print("[3/4] Cargando modelo Random Forest", flush=True)

    modelo = PipelineModel.load(ruta_modelo)

    print("[4/4] Ejecutando inferencia", flush=True)

    predicciones = (
        modelo.transform(datos)
        .withColumn(
            "variacion_estimada",
            F.col("prediccion") - F.col("conexiones")
        )
        .withColumn(
            "variacion_estimada_pct",
            F.when(
                F.col("conexiones") > 0,
                F.col("variacion_estimada")
                / F.col("conexiones") * 100
            )
        )
        .select(
            "departamento",
            F.col("periodo").alias("periodo_origen"),
            "periodo_prediccion",
            F.col("conexiones").alias("conexiones_observadas"),
            F.col("prediccion").alias("conexiones_predichas"),
            "variacion_estimada",
            "variacion_estimada_pct",
            F.lit(ID_MODELO).alias("id_modelo"),
            F.lit("c5cc727").alias("version_datos")
        )
    )

    return predicciones


def publicar_resultados(predicciones, salida, ruta_gold):

    salida = Path(salida).resolve()
    salida.mkdir(parents=True, exist_ok=True)

    registros = [
        fila.asDict()
        for fila in predicciones.orderBy(
            "variacion_estimada_pct"
        ).collect()
    ]

    observado = sum(
        r["conexiones_observadas"] for r in registros
    )

    estimado = sum(
        r["conexiones_predichas"] for r in registros
    )

    reporte = {
        "estado": "INFERENCIA_COMPLETADA",
        "generado_utc": datetime.now(timezone.utc).isoformat(),
        "modelo": ID_MODELO,
        "algoritmo": "Random Forest Regressor",
        "fin_entrenamiento": FIN_ENTRENAMIENTO,
        "fuente_gold": ruta_gold,
        "periodo_origen": registros[0]["periodo_origen"],
        "periodo_prediccion": registros[0]["periodo_prediccion"],
        "territorios": len(registros),
        "conexiones_nacionales_observadas": observado,
        "conexiones_nacionales_estimadas": estimado,
        "variacion_nacional_estimada_pct": (
            (estimado / observado - 1) * 100
        ),
        "predicciones": registros,
        "advertencia": (
            "Estimaciones de conexiones reportadas. "
            "El modelo presenta limitaciones ante cambios abruptos. "
            "No representa cobertura geografica ni usuarios efectivos."
        )
    }

    predicciones.write.mode("overwrite").parquet(
        (salida / "fact_predicciones").as_uri()
    )

    temporal = salida / "predicciones.json.tmp"

    temporal.write_text(
        json.dumps(
            reporte,
            ensure_ascii=False,
            indent=2,
            allow_nan=False
        ),
        encoding="utf-8"
    )

    temporal.replace(salida / "predicciones.json")

    print("\n=== INFERENCIA COMPLETADA ===")
    print("Modelo:", ID_MODELO)
    print("Periodo origen:", reporte["periodo_origen"])
    print("Periodo predicho:", reporte["periodo_prediccion"])
    print("Territorios:", len(registros))
    print("Total observado:", observado)
    print("Total estimado:", round(estimado))
    print("Variacion estimada:",
          round(reporte["variacion_nacional_estimada_pct"], 2), "%")
    print("Resultados:", salida)


def main():

    parser = argparse.ArgumentParser(
        description="Inferencia territorial OSIPTEL"
    )

    parser.add_argument("--gold", required=True)
    parser.add_argument("--modelo", required=True)
    parser.add_argument("--salida", required=True)

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OSIPTEL_Inferencia")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        predicciones = ejecutar_inferencia(
            spark, args.gold, args.modelo
        )

        publicar_resultados(
            predicciones, args.salida, args.gold
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
