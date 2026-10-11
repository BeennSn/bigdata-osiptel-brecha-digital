import json
import os
import re
import subprocess
import tempfile
import unicodedata
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, year, when
from pyspark.sql.types import BooleanType, DateType, LongType, StringType, StructField, StructType

MANIFEST_PATH = "data/manifest.json"
HDFS_SILVER_PATH = "hdfs://localhost:9000/data/silver/conexiones_internet_fijo.parquet"
HDFS_RECHAZADOS_PATH = "hdfs://localhost:9000/data/silver/conexiones_rechazados.parquet"
REPORT_PATH = Path("evidence/reporte_conexiones.json")

EXPECTED_COLUMNS = [
    "Mes",
    "Empresa",
    "Tecnologia",
    "Segmento",
    "Departamento",
    "Rango Velocidad de Bajada (nominal)",
    "Conexiones",
]

OUTPUT_COLUMNS = [
    "mes",
    "empresa",
    "tecnologia",
    "tecnologia_codigo",
    "segmento",
    "departamento",
    "rango_velocidad",
    "rango_velocidad_codigo",
    "conexiones",
    "anio",
    "mes_num",
    "periodo",
    "es_atipico",
    "motivo_rechazo",
]

SCHEMA = StructType(
    [
        StructField("mes", DateType(), True),
        StructField("empresa", StringType(), True),
        StructField("tecnologia", StringType(), True),
        StructField("tecnologia_codigo", StringType(), True),
        StructField("segmento", StringType(), True),
        StructField("departamento", StringType(), True),
        StructField("rango_velocidad", StringType(), True),
        StructField("rango_velocidad_codigo", StringType(), True),
        StructField("conexiones", LongType(), True),
        StructField("anio", StringType(), True),
        StructField("mes_num", StringType(), True),
        StructField("periodo", StringType(), True),
        StructField("es_atipico", BooleanType(), True),
        StructField("motivo_rechazo", StringType(), True),
    ]
)


def _normalizar_texto(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.upper()


def _split_prefijo_categoria(valor):
    if pd.isna(valor):
        return None, None

    texto = str(valor).strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))

    match = re.match(r"^(?P<prefix>[A-Za-z0-9]+)\)\s*(?P<body>.*)$", texto)
    if match:
        prefix = match.group("prefix")
        body = match.group("body").strip()
        return prefix, body
    return None, texto


def _validar_manifesto_xlsx():
    if not os.path.exists(MANIFEST_PATH):
        raise FileNotFoundError(
            f"No se encontró el manifiesto en {MANIFEST_PATH}."
            " Ejecuta la ingesta antes del procesamiento del silver de conexiones."
        )

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    archivos_xlsx = manifest.get("archivos_xlsx", [])
    if not archivos_xlsx:
        raise FileNotFoundError("El manifiesto no contiene archivos XLSX en manifest['archivos_xlsx'].")

    nombre_xlsx = archivos_xlsx[0]
    if not nombre_xlsx.lower().endswith(".xlsx"):
        raise ValueError(f"El archivo listado en el manifiesto no es XLSX: {nombre_xlsx}")

    ruta_hdfs_xlsx = None
    if manifest.get("ruta_hdfs_xlsx"):
        ruta_hdfs_xlsx = manifest["ruta_hdfs_xlsx"].rstrip("/")
    elif manifest.get("ruta_hdfs"):
        ruta_hdfs_xlsx = manifest["ruta_hdfs"].rstrip("/")
    else:
        ruta_hdfs_xlsx = "hdfs://localhost:9000/data/raw"

    return nombre_xlsx, f"{ruta_hdfs_xlsx}/{nombre_xlsx}"


def _obtener_xlsx_local():
    nombre_xlsx, hdfs_xlsx_path = _validar_manifesto_xlsx()
    temp_dir = tempfile.mkdtemp(prefix="conexiones_xlsx_")
    temp_path = os.path.join(temp_dir, nombre_xlsx)

    comando = [
        "hdfs",
        "dfs",
        "-get",
        hdfs_xlsx_path,
        temp_path,
    ]
    try:
        subprocess.run(comando, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"No se pudo obtener el XLSX desde HDFS: {hdfs_xlsx_path}. Detalle: {exc}") from exc

    return temp_path


def _leer_xlsx_pandas(temp_xlsx_path):
    df = pd.read_excel(temp_xlsx_path, sheet_name="Dataset", header=3)

    if df.empty:
        raise ValueError("El archivo XLSX no contiene filas de datos en la hoja Dataset.")

    columnas_reales = [str(col).strip() for col in df.columns]
    columnas_validas = [col for col in columnas_reales if col and col != "Unnamed: 0"]
    columnas_faltantes = [col for col in EXPECTED_COLUMNS if col not in columnas_validas]
    if columnas_faltantes:
        raise ValueError(
            "Faltan columnas esperadas en la hoja Dataset: "
            + ", ".join(columnas_faltantes)
            + ". Columnas reales: "
            + ", ".join(columnas_validas)
        )

    df = df.loc[:, [col for col in df.columns if str(col).strip() not in ["Unnamed: 0", ""]]].copy()
    df.columns = EXPECTED_COLUMNS
    return df


def _aplicar_normalizacion(df):
    df = df.copy()
    df["Mes"] = pd.to_datetime(df["Mes"], errors="coerce")
    df["Empresa"] = df["Empresa"].apply(lambda v: _normalizar_texto(v))
    df["Segmento"] = df["Segmento"].apply(lambda v: _normalizar_texto(v))
    df["Departamento"] = df["Departamento"].apply(lambda v: _normalizar_texto(v))

    tecnologia_prefijo = df["Tecnologia"].apply(lambda v: _split_prefijo_categoria(v)[0])
    tecnologia_body = df["Tecnologia"].apply(lambda v: _split_prefijo_categoria(v)[1])
    df["tecnologia_codigo"] = tecnologia_prefijo
    df["Tecnologia"] = tecnologia_body.apply(lambda v: _normalizar_texto(v))

    rango_prefijo = df["Rango Velocidad de Bajada (nominal)"].apply(lambda v: _split_prefijo_categoria(v)[0])
    rango_body = df["Rango Velocidad de Bajada (nominal)"].apply(lambda v: _split_prefijo_categoria(v)[1])
    df["rango_velocidad_codigo"] = rango_prefijo
    df["Rango Velocidad de Bajada (nominal)"] = rango_body.apply(lambda v: _normalizar_texto(v))

    df["Conexiones"] = pd.to_numeric(df["Conexiones"], errors="coerce").astype("Int64")
    df["Conexiones"] = df["Conexiones"].astype("Int64")

    df = df.rename(
        columns={
            "Mes": "mes",
            "Empresa": "empresa",
            "Tecnologia": "tecnologia",
            "Segmento": "segmento",
            "Departamento": "departamento",
            "Rango Velocidad de Bajada (nominal)": "rango_velocidad",
            "Conexiones": "conexiones",
        }
    )

    return df


def _registrar_calidad(df):
    nulos = df.isna().sum().to_dict()
    total_filas = len(df)
    completitud = {clave: round((1 - (valor / total_filas)) * 100, 2) if total_filas else 0.0 for clave, valor in nulos.items()}
    return {"nulos_por_columna": nulos, "completitud_por_campo": completitud}


def _marcar_atipicos(df):
    df = df.copy()
    if df.empty:
        df["es_atipico"] = False
        return df

    mascaras = []
    for _, grupo in df.groupby(["empresa", "tecnologia"], dropna=False):
        valores = grupo["conexiones"].dropna()
        if valores.empty or len(valores) < 3:
            mascara = pd.Series(False, index=grupo.index)
        else:
            q1 = valores.quantile(0.25)
            q3 = valores.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mascara = grupo["conexiones"].apply(
                lambda x: False if pd.isna(x) else (x < lower or x > upper)
            )
        mascaras.append(mascara)

    df["es_atipico"] = pd.concat(mascaras).sort_index().astype(bool)
    return df


def _generar_reporte(df_total, df_rechazados, duplicados_eliminados, conflictos_clave, atipicos_marcados, nulos_por_columna):
    total_filas = len(df_total)
    rechazo_total = len(df_rechazados)
    report = {
        "filas_leidas": int(total_filas),
        "filas_rechazadas": int(rechazo_total),
        "filas_validas": int(total_filas - rechazo_total),
        "duplicados_eliminados": int(duplicados_eliminados),
        "conflictos_de_clave": int(conflictos_clave),
        "atipicos_marcados": int(atipicos_marcados),
        "nulos_por_columna": {str(k): int(v) for k, v in nulos_por_columna.items()},
        "completitud_por_campo": {
            str(k): round((1 - (v / total_filas)) * 100, 2) if total_filas else 0.0
            for k, v in nulos_por_columna.items()
        },
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report


def procesar_conexiones_a_silver():
    temp_xlsx_path = _obtener_xlsx_local()
    try:
        df = _leer_xlsx_pandas(temp_xlsx_path)
        df = _aplicar_normalizacion(df)

        nulos_por_columna = _registrar_calidad(df)["nulos_por_columna"]
        df["motivo_rechazo"] = None

        condiciones_rechazo = (
            df["mes"].isna()
            | df["empresa"].isna()
            | df["departamento"].isna()
            | df["conexiones"].isna()
        )

        motivos = []
        for _, fila in df.iterrows():
            razones = []
            if pd.isna(fila["mes"]):
                razones.append("mes_nulo")
            if pd.isna(fila["empresa"]):
                razones.append("empresa_nula")
            if pd.isna(fila["departamento"]):
                razones.append("departamento_nulo")
            if pd.isna(fila["conexiones"]):
                razones.append("conexiones_nula")
            motivos.append("; ".join(razones) if razones else None)

        df["motivo_rechazo"] = motivos
        rechazados = df[condiciones_rechazo].copy()

        df_validos = df[~condiciones_rechazo].copy()
        duplicados_eliminados = int(df_validos.duplicated(subset=[
            "mes",
            "empresa",
            "tecnologia",
            "segmento",
            "departamento",
            "rango_velocidad",
            "conexiones",
        ], keep="first").sum())
        df_validos = df_validos.drop_duplicates(
            subset=[
                "mes",
                "empresa",
                "tecnologia",
                "segmento",
                "departamento",
                "rango_velocidad",
                "conexiones",
            ],
            keep="first",
        ).copy()

        for col in ["anio", "mes_num", "periodo", "es_atipico"]:
            if col not in rechazados.columns:
                rechazados[col] = None
        rechazados["es_atipico"] = False
        rechazados["anio"] = rechazados["mes"].dt.strftime("%Y") if "mes" in rechazados.columns else None
        rechazados["mes_num"] = rechazados["mes"].dt.strftime("%m") if "mes" in rechazados.columns else None
        rechazados["periodo"] = rechazados["mes"].dt.strftime("%Y-%m") if "mes" in rechazados.columns else None

        clave_cols = ["mes", "empresa", "tecnologia", "segmento", "departamento", "rango_velocidad"]
        conflictos = (
            df_validos.groupby(clave_cols, dropna=False)["conexiones"]
            .nunique()
            .reset_index(name="n_conexiones_unicas")
        )
        conflictos_clave = int((conflictos["n_conexiones_unicas"] > 1).sum())

        df_validos = _marcar_atipicos(df_validos)
        df_validos["anio"] = df_validos["mes"].dt.strftime("%Y")
        df_validos["mes_num"] = df_validos["mes"].dt.strftime("%m")
        df_validos["periodo"] = df_validos["mes"].dt.strftime("%Y-%m")

        df_validos["motivo_rechazo"] = None

        columnas_finales = [
            "mes",
            "empresa",
            "tecnologia",
            "tecnologia_codigo",
            "segmento",
            "departamento",
            "rango_velocidad",
            "rango_velocidad_codigo",
            "conexiones",
            "anio",
            "mes_num",
            "periodo",
            "es_atipico",
            "motivo_rechazo",
        ]
        df_final = df_validos[columnas_finales].copy()

        spark = SparkSession.builder \
            .appName("Osiptel-Conexiones-Silver") \
            .master(os.getenv("SPARK_MASTER", "yarn")) \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()

        try:
            df_spark = spark.createDataFrame(df_final, schema=SCHEMA)
            df_spark.write.mode("overwrite").format("parquet").option("compression", "snappy").partitionBy("anio").save(HDFS_SILVER_PATH)

            rechazados_final = rechazados.reindex(columns=OUTPUT_COLUMNS, fill_value=None).copy()
            for col in ["anio", "mes_num", "periodo", "es_atipico"]:
                if col not in rechazados_final.columns:
                    rechazados_final[col] = None
            rechazados_final["es_atipico"] = False
            rechazados_final["anio"] = rechazados_final["mes"].dt.strftime("%Y") if "mes" in rechazados_final.columns else None
            rechazados_final["mes_num"] = rechazados_final["mes"].dt.strftime("%m") if "mes" in rechazados_final.columns else None
            rechazados_final["periodo"] = rechazados_final["mes"].dt.strftime("%Y-%m") if "mes" in rechazados_final.columns else None
            rechazados_final = rechazados_final[OUTPUT_COLUMNS]
            rechazados_spark = spark.createDataFrame(rechazados_final, schema=SCHEMA)
            rechazados_spark.write.mode("overwrite").format("parquet").option("compression", "snappy").save(HDFS_RECHAZADOS_PATH)

            report = _generar_reporte(
                df_total=df,
                df_rechazados=rechazados,
                duplicados_eliminados=duplicados_eliminados,
                conflictos_clave=conflictos_clave,
                atipicos_marcados=int(df_validos["es_atipico"].sum()),
                nulos_por_columna=nulos_por_columna,
            )
            print("[INFO] Calidad del archivo XLSX calculada correctamente.")
            print(json.dumps(report, indent=2, ensure_ascii=False))
            return report
        finally:
            spark.stop()
    finally:
        if os.path.exists(temp_xlsx_path):
            os.remove(temp_xlsx_path)
            base_dir = os.path.dirname(temp_xlsx_path)
            if os.path.isdir(base_dir) and not os.listdir(base_dir):
                os.rmdir(base_dir)


if __name__ == "__main__":
    try:
        procesar_conexiones_a_silver()
    except Exception as exc:
        print(f"[ERROR] Falló el procesamiento del silver de conexiones: {exc}")
        raise
