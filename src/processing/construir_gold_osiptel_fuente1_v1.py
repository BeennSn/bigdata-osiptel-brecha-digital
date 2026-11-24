"""Gold operativo OSIPTEL: alertas para revision, no inferencia de causas.

Entrada: snapshot Silver inmutable de Fuente 1.
Salidas: cuatro snapshots Parquet Gold versionados en HDFS.
Comparacion explicita: diciembre 2025 vs marzo 2026 (3 meses).
No usa Fuentes 2-4, pendientes de verificacion semantica para Gold.
"""

import hashlib
import json
from pyspark.sql import SparkSession, Row, functions as F

SOURCE_SHA = "e6e88de8b58ab84f25159d63584300e453d9bb7fe5057f5f31976a602f2ca29a"
SILVER = "hdfs://namenode:8020/data/silver/conexiones_fuente1/v1_" + SOURCE_SHA[:20]
GOLD_BASE = "hdfs://namenode:8020/data/gold"
PERIODO_BASE, PERIODO_ACTUAL = "2025-12", "2026-03"
LOGIC_VERSION = "osiptel-gold-fuente1-v1.0"
PACKAGE = hashlib.sha256(
    (SOURCE_SHA + "|" + PERIODO_BASE + "|" + PERIODO_ACTUAL + "|" + LOGIC_VERSION).encode()
).hexdigest()[:20]


def publicar(spark, tabla, df):
    """Publica via staging + rename; si existe verifica igualdad exacta."""
    base = GOLD_BASE + "/" + tabla
    final = base + "/v1_" + PACKAGE
    tmp = base + "/_staging_v1_" + PACKAGE
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    Path = jvm.org.apache.hadoop.fs.Path
    fs = Path(base).getFileSystem(conf)
    path_base, path_final, path_tmp = Path(base), Path(final), Path(tmp)
    if not fs.exists(path_base) and not fs.mkdirs(path_base):
        raise RuntimeError("No se puede crear directorio de tabla: " + base)

    def verificar_igualdad(ruta):
        publicado = spark.read.parquet(ruta)
        columnas = sorted(df.columns)

        if sorted(publicado.columns) != columnas:
            raise RuntimeError("Columnas diferentes: " + tabla)

        esperadas = df.select(*columnas)
        observadas = publicado.select(*columnas)

        tipos_e = [(c.name, c.dataType.simpleString()) for c in esperadas.schema.fields]
        tipos_o = [(c.name, c.dataType.simpleString()) for c in observadas.schema.fields]

        if tipos_e != tipos_o:
            raise RuntimeError("Tipos incompatibles: " + tabla)

        def firma(datos):
            huella = F.sha2(
                F.to_json(
                    F.struct(*[F.col(c) for c in columnas]),
                    {"ignoreNullFields": "false"}
                ), 256
            )
            hashes = datos.select(huella.alias("hash_fila"))
            agregados = [F.count("*").alias("filas")]

            for pos in (1, 16, 31, 46):
                fragmento = F.conv(
                    F.substring("hash_fila", pos, 15), 16, 10
                )
                agregados.append(
                    F.sum(fragmento.cast("decimal(38,0)")).alias("firma_" + str(pos))
                )

            return tuple(hashes.agg(*agregados).first())

        esperado = firma(esperadas)
        observado = firma(observadas)

        print("CONTROL GOLD:", tabla,
              "esperado=", esperado,
              "observado=", observado, flush=True)

        if esperado != observado:
            raise RuntimeError("Diferencia de integridad: " + tabla)

        return int(observado[0])

    if fs.exists(path_final):
        n = verificar_igualdad(final)
        estado = "EXISTENTE_VERIFICADO"
    else:
        if fs.exists(path_tmp):
            print("STAGING EXISTENTE; verificando integridad antes de recuperarlo: " + tmp, flush=True)
        else:
            df.coalesce(1).write.mode("errorifexists").parquet(tmp)
        n = verificar_igualdad(tmp)
        if not fs.rename(path_tmp, path_final):
            raise RuntimeError("Fallo el rename atomico hacia " + final)
        estado = "PUBLICADO_Y_VERIFICADO"
    print(f"GOLD {tabla}: {estado}, {n} filas, {final}", flush=True)
    return {"tabla": tabla, "estado": estado, "filas": n, "ruta_hdfs": final}


def main():
    spark = (SparkSession.builder
        .appName("OSIPTEL_Gold_Fuente1_Alertas_V1")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.parquet(SILVER)
        columnas = {"periodo", "conexiones", "empresa_normalizada", "departamento_normalizado",
                    "segmento_normalizado", "tecnologia_normalizada", "estado_validacion", "sha256_origen"}
        faltantes = sorted(columnas - set(df.columns))
        if faltantes:
            raise RuntimeError("Silver no tiene columnas requeridas: " + str(faltantes))

        auditoria = df.agg(
            F.count("*").alias("filas"),
            F.countDistinct("periodo").alias("periodos"),
            F.countDistinct("sha256_origen").alias("shas"),
            F.min("sha256_origen").alias("sha"),
            F.sum(F.when(F.col("conexiones").isNull() | (F.col("conexiones") < 0), 1).otherwise(0)).alias("conexiones_invalidas"),
            F.sum(F.when(F.col("estado_validacion") == "RECHAZADO", 1).otherwise(0)).alias("rechazados")
        ).first()
        if (auditoria.filas != 141106 or auditoria.periodos != 71 or
            auditoria.shas != 1 or auditoria.sha != SOURCE_SHA or
            auditoria.conexiones_invalidas != 0 or auditoria.rechazados != 0):
            raise RuntimeError("No coincide el snapshot Silver con la auditoria validada: " + str(auditoria))

        d = df.where(F.col("periodo").isin(PERIODO_BASE, PERIODO_ACTUAL))
        totales = {r["periodo"]: int(r["total"]) for r in
                   d.groupBy("periodo").agg(F.sum("conexiones").alias("total")).collect()}
        if totales != {PERIODO_BASE: 4379544, PERIODO_ACTUAL: 4135995}:
            raise RuntimeError("Totales de periodos inesperados; no publicar alertas: " + str(totales))
        base, actual = totales[PERIODO_BASE], totales[PERIODO_ACTUAL]
        cambio = actual - base
        porcentaje = round(cambio * 100.0 / base, 4)

        kpis = spark.createDataFrame([Row(
            periodo_base=PERIODO_BASE, periodo_actual=PERIODO_ACTUAL,
            conexiones_base=base, conexiones_actual=actual,
            delta_conexiones=cambio, variacion_porcentual=porcentaje,
            meses_entre_periodos=3, universo="CONEXIONES_FIJAS_REPORTADAS_OSIPTEL",
            tipo_decision="VALIDAR_VARIACION_EN_REGISTROS_REPORTADOS",
            regla_version=LOGIC_VERSION, origen_sha256=SOURCE_SHA
        )])

        # Las dimensiones vacias se excluyen del panel territorial, sin afectar el KPI nacional.
        dims = d.withColumn(
            "operador", F.when(F.col("empresa_normalizada").isNull() |
                (F.trim("empresa_normalizada") == ""), F.lit("SIN_OPERADOR"))
                .otherwise(F.col("empresa_normalizada"))
        ).withColumn(
            "departamento", F.when(F.col("departamento_normalizado").isNull() |
                (F.trim("departamento_normalizado") == ""), F.lit("SIN_DEPARTAMENTO"))
                .otherwise(F.col("departamento_normalizado"))
        )

        por_periodo = (dims.groupBy("operador", "departamento", "periodo")
            .agg(F.sum("conexiones").cast("long").alias("total")))
        tabla_base = (por_periodo.where(F.col("periodo") == PERIODO_BASE)
            .select("operador", "departamento", F.col("total").alias("conexiones_base")))
        tabla_actual = (por_periodo.where(F.col("periodo") == PERIODO_ACTUAL)
            .select("operador", "departamento", F.col("total").alias("conexiones_actual")))

        panel = (tabla_base.join(tabla_actual, ["operador", "departamento"], "full_outer")
            .fillna({"conexiones_base": 0, "conexiones_actual": 0})
            .withColumn("delta_conexiones", F.col("conexiones_actual") - F.col("conexiones_base"))
            .withColumn("variacion_porcentual", F.when(F.col("conexiones_base") > 0,
                F.round(F.col("delta_conexiones") * F.lit(100.0) / F.col("conexiones_base"), 4))
                .otherwise(F.lit(None).cast("double")))
            .withColumn("periodo_base", F.lit(PERIODO_BASE))
            .withColumn("periodo_actual", F.lit(PERIODO_ACTUAL))
            .withColumn("regla_version", F.lit(LOGIC_VERSION))
            .withColumn("origen_sha256", F.lit(SOURCE_SHA)))

        # Alerta operativa: diferencia de magnitud y variacion relativa, con denominador suficiente.
        alertas = (panel.where(
            (F.col("departamento") != "SIN_DEPARTAMENTO") &
            (F.col("operador") != "SIN_OPERADOR") &
            (F.col("conexiones_base") >= 1000) &
            (F.abs(F.col("delta_conexiones")) >= 1000) &
            (F.abs(F.col("variacion_porcentual")) >= 15.0)
        ).withColumn("prioridad", F.when(
            (F.abs(F.col("delta_conexiones")) >= 10000) &
            (F.abs(F.col("variacion_porcentual")) >= 20), "ALTA")
            .otherwise("MEDIA"))
        .withColumn("accion_sugerida", F.lit(
            "Contrastar la variacion con reportes del operador, cambios de carga y metodologia; no inferir bajas reales"))
        .withColumn("estado", F.lit("ALERTA_ESTADISTICA_PENDIENTE_DE_REVISION")))

        residencia = dims.where(
            (F.col("periodo") == PERIODO_ACTUAL) &
            (F.col("departamento") != "SIN_DEPARTAMENTO") &
            F.upper(F.coalesce(F.col("segmento_normalizado"), F.lit(""))).contains("RESIDENCIAL")
        )
        por_depto = (residencia.groupBy("departamento")
            .agg(F.sum("conexiones").cast("long").alias("conexiones_residenciales"),
                 F.sum(F.when(F.upper(F.coalesce(F.col("tecnologia_normalizada"), F.lit("")))
                     .rlike("FIBRA|FTTH"), F.col("conexiones")).otherwise(0))
                     .cast("long").alias("conexiones_fibra_residenciales"))
            .withColumn("participacion_fibra_pct", F.when(
                F.col("conexiones_residenciales") > 0,
                F.round(F.col("conexiones_fibra_residenciales") * 100.0 /
                    F.col("conexiones_residenciales"), 2)))
            .withColumn("periodo", F.lit(PERIODO_ACTUAL))
            .withColumn("definicion", F.lit(
                "Porcentaje fibra sobre conexiones fijas residenciales registradas; NO es cobertura poblacional"))
            .withColumn("origen_sha256", F.lit(SOURCE_SHA)))

        # Comprobaciones antes de publicar: comparacion nacional exacta y conteos no negativos.
        if (base <= 0 or actual <= 0 or
            alertas.where((F.col("conexiones_base") < 0) |
                          (F.col("conexiones_actual") < 0)).limit(1).count()):
            raise RuntimeError("Fallaron controles de Gold")

        salida = {
            "entrada_silver": SILVER,
            "version_reglas": LOGIC_VERSION,
            "periodo_base": PERIODO_BASE,
            "periodo_actual": PERIODO_ACTUAL,
            "conexiones_base": base,
            "conexiones_actual": actual,
            "variacion_abs": cambio,
            "variacion_porcentual": porcentaje,
            "restriccion": "Alertas de reporte, NO bajas reales demostradas. Participacion fibra NO equivale a cobertura.",
            "tablas": {}
        }
        salida["tablas"]["kpis"] = publicar(spark, "osiptel_kpis_nacionales_f1", kpis)
        salida["tablas"]["panel"] = publicar(spark, "osiptel_variacion_operador_departamento_f1", panel)
        salida["tablas"]["alertas"] = publicar(spark, "osiptel_alertas_revision_f1", alertas)
        salida["tablas"]["fibra"] = publicar(spark, "osiptel_participacion_fibra_residencial_f1", por_depto)
        print("\n=== GOLD OSIPTEL FUENTE 1 ===")
        print(json.dumps(salida, indent=2, ensure_ascii=False))
        print("\n=== ALERTAS PRINCIPALES (muestra; no prueba bajas reales) ===")
        alertas.select("operador", "departamento", "conexiones_base",
                       "conexiones_actual", "delta_conexiones", "variacion_porcentual", "prioridad")\
            .orderBy(F.abs(F.col("delta_conexiones")).desc()).show(12, truncate=45)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
