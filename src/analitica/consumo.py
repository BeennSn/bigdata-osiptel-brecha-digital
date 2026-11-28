
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ID_MODELO = "RF_A_operativo_c5cc727_v1"


def construir_tablas(spark, args):

    nacional = spark.read.parquet(args.nacional)

    territorial = (
        spark.read.parquet(args.territorial)
        .filter(F.col("periodo") >= "2016-03")
    )

    hogares = (
        spark.read.parquet(args.hogares)
        .withColumn(
            "periodo_cierre",
            F.concat_ws(
                "-",
                F.substring("periodo", 1, 4),
                F.lpad(
                    (F.substring("periodo", 7, 1)
                     .cast("int") * 3).cast("string"),
                    2, "0"
                )
            )
        )
        .select(
            F.col("periodo").alias("trimestre_inei"),
            F.col("periodo_cierre").alias("periodo"),
            "ambito", "indicador", "porcentaje",
            "unidad", "pdf", "pagina_pdf", "sha256_pdf"
        )
    )

    predicciones = spark.read.parquet(args.predicciones)

    tablas = {
        "fact_conexiones_nacional": nacional,
        "fact_conexiones_territorial": territorial,
        "fact_hogares_internet": hogares,
        "fact_predicciones": predicciones
    }


    periodos = [
        nacional.select("periodo"),
        territorial.select("periodo"),
        hogares.select("periodo"),
        predicciones.select(
            F.col("periodo_origen").alias("periodo")
        ),
        predicciones.select(
            F.col("periodo_prediccion").alias("periodo")
        )
    ]

    tablas["dim_periodo"] = (
        reduce(lambda a, b: a.unionByName(b), periodos)
        .distinct()
        .withColumn(
            "fecha",
            F.last_day(F.to_date(
                F.concat(F.col("periodo"), F.lit("-01"))
            ))
        )
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("trimestre", F.quarter("fecha"))
        .withColumn(
            "anio_trimestre",
            F.concat("anio", F.lit("-T"), "trimestre")
        )
    )

    dimensiones = {
        "dim_territorio": "departamento",
        "dim_operador": "operador",
        "dim_tecnologia": "tecnologia",
        "dim_segmento": "segmento",
        "dim_velocidad": "rango_velocidad"
    }

    for nombre, columna in dimensiones.items():
        tablas[nombre] = territorial.select(columna).distinct()

    tablas["dim_ambito"] = hogares.select("ambito").distinct()

    tablas["dim_modelo"] = spark.createDataFrame(
        [(
            ID_MODELO, "Random Forest Regressor",
            50, 3, 42, "2026-03", "Operativo", "c5cc727"
        )],
        [
            "id_modelo", "algoritmo", "num_arboles",
            "profundidad_maxima", "semilla",
            "fin_entrenamiento", "tipo_modelo",
            "version_datos"
        ]
    )

    return tablas


def validar_tablas(tablas):

    claves = {
        "fact_conexiones_nacional": ["periodo"],
        "fact_conexiones_territorial": [
            "periodo", "departamento", "operador",
            "tecnologia", "segmento", "rango_velocidad"
        ],
        "fact_hogares_internet": [
            "periodo", "ambito", "indicador"
        ],
        "fact_predicciones": [
            "departamento", "periodo_origen",
            "periodo_prediccion", "id_modelo"
        ],
        "dim_periodo": ["periodo"],
        "dim_territorio": ["departamento"],
        "dim_operador": ["operador"],
        "dim_tecnologia": ["tecnologia"],
        "dim_segmento": ["segmento"],
        "dim_velocidad": ["rango_velocidad"],
        "dim_ambito": ["ambito"],
        "dim_modelo": ["id_modelo"]
    }

    inventario = []

    for nombre, df in tablas.items():

        total = df.count()

        if df.dropna(subset=claves[nombre]).count() != total:
            raise ValueError(f"Claves nulas: {nombre}")

        if df.select(*claves[nombre]).distinct().count() != total:
            raise ValueError(f"Claves duplicadas: {nombre}")

        inventario.append({
            "tabla": nombre,
            "filas": total,
            "columnas": len(df.columns)
        })

    relaciones = [
        ("fact_conexiones_nacional", "periodo", "dim_periodo"),
        ("fact_conexiones_territorial", "periodo", "dim_periodo"),
        ("fact_conexiones_territorial", "departamento", "dim_territorio"),
        ("fact_conexiones_territorial", "operador", "dim_operador"),
        ("fact_conexiones_territorial", "tecnologia", "dim_tecnologia"),
        ("fact_conexiones_territorial", "segmento", "dim_segmento"),
        ("fact_conexiones_territorial", "rango_velocidad", "dim_velocidad"),
        ("fact_hogares_internet", "periodo", "dim_periodo"),
        ("fact_hogares_internet", "ambito", "dim_ambito"),
        ("fact_predicciones", "departamento", "dim_territorio"),
        ("fact_predicciones", "periodo_origen", "dim_periodo"),
        ("fact_predicciones", "periodo_prediccion", "dim_periodo"),
        ("fact_predicciones", "id_modelo", "dim_modelo")
    ]

    for hecho, fk, dimension in relaciones:

        h = tablas[hecho]
        d = tablas[dimension]
        pk = claves[dimension][0]

        if h.join(d, h[fk] == d[pk], "left_anti").count():
            raise ValueError(
                f"Registros huerfanos: {hecho} -> {dimension}"
            )

    print("Relaciones validadas:", len(relaciones))

    return inventario


def calcular_kpis(tablas):

    nacional = tablas["fact_conexiones_nacional"]
    territorial = tablas["fact_conexiones_territorial"]
    hogares = tablas["fact_hogares_internet"]
    pred = tablas["fact_predicciones"]

    # KPI 1 y 2: stock y variacion trimestral

    cierres = (
        nacional
        .filter(F.substring("periodo", 6, 2).isin(
            "03", "06", "09", "12"
        ))
        .orderBy(F.desc("periodo"))
        .limit(2)
        .collect()
    )

    if len(cierres) != 2:
        raise ValueError("Historial trimestral insuficiente")

    actual, anterior = cierres

    periodo = actual["periodo"]
    total = int(actual["conexiones_fijas"])
    previo = int(anterior["conexiones_fijas"])

    esperado = (
        nacional.filter(F.col("periodo") == periodo)
        .select(F.date_format(F.add_months(
            F.to_date(F.concat("periodo", F.lit("-01"))),
            -3
        ), "yyyy-MM").alias("previo"))
        .first()["previo"]
    )

    if anterior["periodo"] != esperado or previo <= 0 or total <= 0:
        raise ValueError("Cierres nacionales incompatibles")

    variacion = (total / previo - 1) * 100

    # KPI 3: participacion de fibra optica

    corte = territorial.filter(F.col("periodo") == periodo)

    stock_territorial = corte.agg(
        F.sum("conexiones_fijas")
    ).first()[0]

    if stock_territorial != total:
        raise ValueError("F1 nacional y territorial no coinciden")

    fibra = corte.filter(
        F.col("tecnologia") == "5) FIBRA ÓPTICA"
    ).agg(F.sum("conexiones_fijas")).first()[0]

    participacion = fibra / total * 100 if fibra is not None else None

    # KPI 4: brecha de acceso a internet en hogares

    periodo_inei = hogares.agg(
        F.max("periodo")
    ).first()[0]

    ambitos = {
        x["ambito"]: float(x["porcentaje"])
        for x in hogares.filter(
            F.col("periodo") == periodo_inei
        ).collect()
    }

    brecha = (
        ambitos["Lima Metropolitana"] - ambitos["Area rural"]
    )

    # KPI 5: variacion estimada por Random Forest

    resumen = pred.agg(
        F.count("*").alias("n"),
        F.countDistinct("departamento").alias("territorios"),
        F.countDistinct("periodo_origen").alias("origenes"),
        F.countDistinct("periodo_prediccion").alias("destinos"),
        F.max("periodo_origen").alias("origen"),
        F.max("periodo_prediccion").alias("destino"),
        F.sum("conexiones_observadas").alias("observado"),
        F.sum("conexiones_predichas").alias("estimado")
    ).first()

    if (
        resumen["n"] != 25
        or resumen["territorios"] != 25
        or resumen["origenes"] != 1
        or resumen["destinos"] != 1
        or resumen["origen"] != periodo
        or resumen["observado"] != total
    ):
        raise ValueError("Predicciones incompatibles con Gold")

    estimado = float(resumen["estimado"])
    variacion_estimada = (estimado / total - 1) * 100

    def kpi(nombre, valor, unidad, corte, formula,
            frecuencia, responsable, decision):

        return {
            "nombre": nombre,
            "valor": round(valor, 2) if valor is not None else None,
            "unidad": unidad,
            "periodo": corte,
            "formula": formula,
            "frecuencia": frecuencia,
            "responsable": responsable,
            "decision": decision
        }

    return [
        kpi(
            "Conexiones fijas nacionales", total,
            "conexiones", periodo,
            "Stock nacional al cierre",
            "Trimestral", "Analista de mercado",
            "Monitorear la evolucion del servicio"
        ),
        kpi(
            "Variacion trimestral", variacion,
            "%", periodo,
            "(Stock actual / stock anterior - 1) * 100",
            "Trimestral", "Analista de supervision",
            "Investigar disminuciones relevantes"
        ),
        kpi(
            "Participacion de fibra optica", participacion,
            "%", periodo,
            "Conexiones fibra / conexiones totales * 100",
            "Trimestral", "Especialista en telecomunicaciones",
            "Evaluar la transicion tecnologica"
        ),
        kpi(
            "Brecha Lima Metropolitana - rural", brecha,
            "pp", periodo_inei,
            "Porcentaje Lima Metropolitana - porcentaje rural",
            "Trimestral", "Especialista en politicas publicas",
            "Priorizar estudios sobre acceso rural"
        ),
        kpi(
            "Variacion nacional estimada", variacion_estimada,
            "%", resumen["destino"],
            "(Conexiones predichas / observadas - 1) * 100",
            "Cada nuevo corte de F1", "Analista de planificacion",
            "Priorizar territorios para seguimiento"
        )
    ]


def main():

    parser = argparse.ArgumentParser(
        description="Capa de consumo analitico OSIPTEL"
    )

    for parametro in [
        "nacional", "territorial", "hogares",
        "predicciones", "salida"
    ]:
        parser.add_argument("--" + parametro, required=True)

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OSIPTEL_Capa_Consumo")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        print("[1/4] Construyendo modelo dimensional", flush=True)
        tablas = construir_tablas(spark, args)

        print("[2/4] Validando tablas y relaciones", flush=True)
        inventario = validar_tablas(tablas)

        print("[3/4] Calculando cinco KPIs", flush=True)
        kpis = calcular_kpis(tablas)

        print("[4/4] Publicando capa de consumo", flush=True)

        salida = Path(args.salida).resolve()
        salida.mkdir(parents=True, exist_ok=True)

        for nombre, df in tablas.items():
            df.write.mode("overwrite").parquet(
                (salida / nombre).as_uri()
            )

        reporte = {
            "estado": "CONSUMO_COMPLETADO",
            "generado_utc": datetime.now(timezone.utc).isoformat(),
            "modelo": ID_MODELO,
            "tablas": inventario,
            "kpis": kpis,
            "advertencia": (
                "Los responsables son roles propuestos. "
                "Los indicadores observados y estimados "
                "tienen periodos y unidades diferentes. "
                "Las predicciones no son valores confirmados."
            )
        }

        temporal = salida / "kpis.json.tmp"

        temporal.write_text(
            json.dumps(
                reporte, ensure_ascii=False,
                indent=2, allow_nan=False
            ),
            encoding="utf-8"
        )

        temporal.replace(salida / "kpis.json")

        print("\n=== CAPA DE CONSUMO COMPLETADA ===")
        print("Tablas generadas:", len(tablas))

        for x in kpis:
            print(
                x["nombre"], "|",
                x["valor"], x["unidad"], "|",
                x["periodo"]
            )

        print("Resultados:", salida)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
