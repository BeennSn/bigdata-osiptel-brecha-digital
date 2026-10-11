import json
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
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


def _hdfs_path_existe(ruta_hdfs: str) -> bool:
    """Verifica si una ruta existe en HDFS mediante hdfs dfs -test -e."""
    try:
        res = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", ruta_hdfs],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return res.returncode == 0
    except Exception:
        return False


def _normalizar_texto(value):
    """Normaliza cadenas de texto: trim, espacios colapsados, NFKD sin tildes y mayúsculas."""
    if pd.isna(value):
        return None
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.upper()


def _split_prefijo_categoria(valor):
    """Extrae prefijos tipo '7)' o 'a)' conservando el código y limpiando la categoría."""
    if pd.isna(valor):
        return None, None

    texto = str(valor).strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))

    match = re.match(r"^(?P<prefix>[A-Za-z0-9]+)\)\s*(?P<body>.*)$", texto)
    if match:
        prefix = match.group("prefix").strip().upper()
        body = match.group("body").strip().upper()
        return prefix, body
    return None, texto.upper()


def _resolver_ruta_hdfs_xlsx(manifest: dict) -> tuple:
    """Resuelve la ubicación del archivo XLSX dentro de HDFS a partir del manifiesto."""
    archivos_xlsx = manifest.get("archivos_xlsx", [])
    if not archivos_xlsx:
        raise FileNotFoundError(
            "El manifiesto no contiene archivos XLSX en manifest['archivos_xlsx']."
        )

    nombre_xlsx = archivos_xlsx[0]
    candidatas = []

    # 1. Ruta desde sección datasets si existe
    if manifest.get("datasets", {}).get("internet_fijo", {}).get("ruta_hdfs"):
        candidatas.append(f"{manifest['datasets']['internet_fijo']['ruta_hdfs'].rstrip('/')}/{nombre_xlsx}")
    # 2. Misma carpeta de HDFS Raw que los PDFs
    if manifest.get("ruta_hdfs"):
        candidatas.append(f"{manifest['ruta_hdfs'].rstrip('/')}/{nombre_xlsx}")
    # 3. Ruta dedicada en HDFS si existe en manifiesto
    if manifest.get("ruta_hdfs_xlsx"):
        candidatas.append(f"{manifest['ruta_hdfs_xlsx'].rstrip('/')}/{nombre_xlsx}")
    # 3. Rutas estándar de respaldo
    candidatas.extend(
        [
            f"/data/raw/osiptel_pdfs/{nombre_xlsx}",
            f"/data/raw/conexiones_internet_fijo/{nombre_xlsx}",
            f"/data/raw/{nombre_xlsx}",
        ]
    )

    for ruta in candidatas:
        if _hdfs_path_existe(ruta):
            return nombre_xlsx, ruta

    # Si ninguna respondió exitosa aún (o HDFS se está iniciando), usar la primera candidata
    ruta_elegida = candidatas[0] if candidatas else f"/data/raw/osiptel_pdfs/{nombre_xlsx}"
    return nombre_xlsx, ruta_elegida


def _obtener_xlsx_local() -> tuple:
    """Descarga el XLSX desde HDFS a una carpeta temporal local usando una lista de argumentos."""
    if not os.path.exists(MANIFEST_PATH):
        raise FileNotFoundError(
            f"No se encontró el manifiesto en {MANIFEST_PATH}. Ejecuta la ingesta previa."
        )

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    nombre_xlsx, hdfs_path = _resolver_ruta_hdfs_xlsx(manifest)
    temp_dir = tempfile.mkdtemp(prefix="conexiones_xlsx_")
    temp_path = os.path.join(temp_dir, nombre_xlsx)

    print(f"[INFO] Obteniendo XLSX desde HDFS: '{hdfs_path}' -> '{temp_path}'")
    # Subprocess con lista de argumentos para soportar espacios en el nombre
    comando = ["hdfs", "dfs", "-get", hdfs_path, temp_path]
    try:
        subprocess.run(comando, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        # Si falla HDFS (ej. entorno de pruebas local sin Hadoop), buscar en staging local
        rutas_locales = [
            os.path.join("data", "raw", "datos", "internet_fijo", nombre_xlsx),
            os.path.join("data", "raw", nombre_xlsx),
        ]
        copia_encontrada = None
        for rl in rutas_locales:
            if os.path.exists(rl):
                copia_encontrada = rl
                break

        if copia_encontrada:
            print(
                f"[ADVERTENCIA] No se pudo obtener desde HDFS ({exc}). "
                f"Usando copia local de respaldo: '{copia_encontrada}'"
            )
            shutil.copy2(copia_encontrada, temp_path)
        else:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise RuntimeError(
                f"No se pudo obtener el XLSX desde HDFS ({hdfs_path}) ni se encontró localmente. Error: {exc}"
            ) from exc

    return temp_path, temp_dir


def _leer_xlsx_pandas(xlsx_path: str) -> pd.DataFrame:
    """Lee la hoja Dataset del XLSX con openpyxl, validando encabezado y esquema."""
    print(f"[INFO] Leyendo '{xlsx_path}' con pandas y openpyxl (hoja 'Dataset', fila 4)...")
    df = pd.read_excel(xlsx_path, sheet_name="Dataset", header=3, engine="openpyxl")

    if df.empty:
        raise ValueError("El archivo XLSX no contiene datos en la hoja 'Dataset'.")

    # Verificación de esquema
    columnas_reales = [str(col).strip() for col in df.columns]
    columnas_validas = [col for col in columnas_reales if col and col != "Unnamed: 0"]
    columnas_faltantes = [col for col in EXPECTED_COLUMNS if col not in columnas_validas]
    if columnas_faltantes:
        raise ValueError(
            f"Faltan columnas esperadas en la hoja Dataset: {columnas_faltantes}. "
            f"Columnas encontradas: {columnas_validas}"
        )

    # Filtrar columna vacía Unnamed: 0 y mapear columnas exactas
    df = df.loc[
        :, [col for col in df.columns if str(col).strip() not in ["Unnamed: 0", ""]]
    ].copy()
    df.columns = EXPECTED_COLUMNS
    return df


def _marcar_atipicos(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula IQR por (empresa, tecnologia) y marca columna booleana es_atipico sin eliminar."""
    df = df.copy()
    if df.empty:
        df["es_atipico"] = False
        return df

    stats = (
        df.groupby(["empresa", "tecnologia"], dropna=False)["conexiones"]
        .agg(
            q1=lambda s: s.quantile(0.25),
            q3=lambda s: s.quantile(0.75),
            count="count",
        )
        .reset_index()
    )

    stats["iqr"] = stats["q3"] - stats["q1"]
    stats["lower"] = stats["q1"] - 1.5 * stats["iqr"]
    stats["upper"] = stats["q3"] + 1.5 * stats["iqr"]

    merged = df.merge(
        stats[["empresa", "tecnologia", "lower", "upper", "count"]],
        on=["empresa", "tecnologia"],
        how="left",
    )

    es_atipico = (merged["count"] >= 3) & (
        (merged["conexiones"] < merged["lower"])
        | (merged["conexiones"] > merged["upper"])
    )
    df["es_atipico"] = es_atipico.fillna(False).to_numpy(dtype=bool)
    return df


def _generar_reporte(
    total_filas: int,
    rechazados_count: int,
    validos_count: int,
    duplicados_eliminados: int,
    conflictos_clave: int,
    atipicos_marcados: int,
    nulos_por_columna: dict,
    completitud_por_campo: dict,
) -> dict:
    """Escribe y retorna el reporte de calidad dinámico en evidence/reporte_conexiones.json."""
    reporte = {
        "filas_leidas": int(total_filas),
        "filas_rechazadas": int(rechazados_count),
        "filas_validas": int(validos_count),
        "duplicados_eliminados": int(duplicados_eliminados),
        "conflictos_de_clave": int(conflictos_clave),
        "atipicos_marcados": int(atipicos_marcados),
        "nulos_por_columna": {str(k): int(v) for k, v in nulos_por_columna.items()},
        "completitud_por_campo": {
            str(k): round(float(v), 2) for k, v in completitud_por_campo.items()
        },
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    return reporte


def procesar_conexiones_a_silver(forzar: bool = False):
    """Procesa el dataset XLSX de conexiones de internet fijo hacia la capa Silver en HDFS."""
    print("=== INICIANDO PROCESAMIENTO SILVER: CONEXIONES INTERNET FIJO (SPARK + HDFS) ===")

    # 1. Verificación de persistencia previa
    if (
        not forzar
        and _hdfs_path_existe(HDFS_SILVER_PATH)
        and _hdfs_path_existe(HDFS_RECHAZADOS_PATH)
        and REPORT_PATH.exists()
    ):
        print(f"[INFO] La capa Silver de conexiones ya existe en HDFS ({HDFS_SILVER_PATH}).")
        print(f"[INFO] Reporte existente en {REPORT_PATH}.")
        print("[INFO] Se omite el procesamiento por persistencia. Usa forzar=True si requieres reprocesar.")
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    # 2. Descarga a temporal local desde HDFS
    temp_xlsx_path, temp_dir = _obtener_xlsx_local()

    try:
        # 3. Lectura y validación de esquema con pandas
        df_raw = _leer_xlsx_pandas(temp_xlsx_path)
        total_filas = len(df_raw)

        # 4. Cálculo dinámico de nulos y completitud sobre columnas originales
        nulos_por_columna = {col: int(df_raw[col].isna().sum()) for col in EXPECTED_COLUMNS}
        completitud_por_campo = {
            col: round((1 - (nulos_por_columna[col] / total_filas)) * 100, 2)
            if total_filas > 0
            else 0.0
            for col in EXPECTED_COLUMNS
        }

        # 5. Tipos y normalización de texto y categorías
        df = df_raw.copy()
        df["mes_dt"] = pd.to_datetime(df["Mes"], errors="coerce")
        df["empresa"] = df["Empresa"].apply(_normalizar_texto)
        df["segmento"] = df["Segmento"].apply(_normalizar_texto)
        df["departamento"] = df["Departamento"].apply(_normalizar_texto)

        # Separar código y categoría en Tecnología
        df["tecnologia_codigo"] = df["Tecnologia"].apply(
            lambda v: _split_prefijo_categoria(v)[0]
        )
        df["tecnologia"] = df["Tecnologia"].apply(
            lambda v: _split_prefijo_categoria(v)[1]
        )

        # Separar código y categoría en Rango Velocidad
        df["rango_velocidad_codigo"] = df["Rango Velocidad de Bajada (nominal)"].apply(
            lambda v: _split_prefijo_categoria(v)[0]
        )
        df["rango_velocidad"] = df["Rango Velocidad de Bajada (nominal)"].apply(
            lambda v: _split_prefijo_categoria(v)[1]
        )

        # Conexiones a entero numérico
        df["conexiones"] = pd.to_numeric(df["Conexiones"], errors="coerce").astype("Int64")

        # 6. Detección de nulos para cuarentena (mes, empresa, departamento o conexiones)
        condiciones_rechazo = (
            df["mes_dt"].isna()
            | df["empresa"].isna()
            | df["departamento"].isna()
            | df["conexiones"].isna()
        )

        # Preparación de registros rechazados con su motivo
        df_rechazados = df[condiciones_rechazo].copy()
        motivos = []
        for _, fila in df_rechazados.iterrows():
            razones = []
            if pd.isna(fila["mes_dt"]):
                razones.append("mes_nulo")
            if pd.isna(fila["empresa"]):
                razones.append("empresa_nula")
            if pd.isna(fila["departamento"]):
                razones.append("departamento_nulo")
            if pd.isna(fila["conexiones"]):
                razones.append("conexiones_nula")
            motivos.append("; ".join(razones) if razones else "desconocido")
        df_rechazados["motivo_rechazo"] = motivos

        df_rechazados["mes"] = df_rechazados["mes_dt"].dt.date
        df_rechazados["anio"] = df_rechazados["mes_dt"].dt.strftime("%Y")
        df_rechazados["mes_num"] = df_rechazados["mes_dt"].dt.strftime("%m")
        df_rechazados["periodo"] = df_rechazados["mes_dt"].dt.strftime("%Y-%m")
        df_rechazados["es_atipico"] = False

        # Preparación de registros válidos
        df_validos = df[~condiciones_rechazo].copy()

        # 7. Duplicados: eliminar solo exactos
        cols_dedup = [
            "mes_dt",
            "empresa",
            "tecnologia",
            "segmento",
            "departamento",
            "rango_velocidad",
            "conexiones",
        ]
        duplicados_eliminados = int(df_validos.duplicated(subset=cols_dedup, keep="first").sum())
        df_validos = df_validos.drop_duplicates(subset=cols_dedup, keep="first").copy()

        # Conflictos de clave: misma clave con distinto valor de conexiones (no eliminar)
        clave_cols = [
            "mes_dt",
            "empresa",
            "tecnologia",
            "segmento",
            "departamento",
            "rango_velocidad",
        ]
        confl = (
            df_validos.groupby(clave_cols, dropna=False)["conexiones"]
            .nunique()
            .reset_index(name="n_conexiones")
        )
        conflictos_clave = int((confl["n_conexiones"] > 1).sum())

        # 8. Atípicos: marcar IQR por empresa + tecnología (sin eliminar)
        df_validos = _marcar_atipicos(df_validos)
        atipicos_marcados = int(df_validos["es_atipico"].sum())

        # 9. Columnas temporales derivadas
        df_validos["mes"] = df_validos["mes_dt"].dt.date
        df_validos["anio"] = df_validos["mes_dt"].dt.strftime("%Y")
        df_validos["mes_num"] = df_validos["mes_dt"].dt.strftime("%m")
        df_validos["periodo"] = df_validos["mes_dt"].dt.strftime("%Y-%m")
        df_validos["motivo_rechazo"] = None

        # Alinear DataFrames finales con el orden exacto de OUTPUT_COLUMNS
        df_silver_final = df_validos[OUTPUT_COLUMNS].copy()
        df_rechazados_final = df_rechazados[OUTPUT_COLUMNS].copy()

        # Asegurar NoneType puro en celdas nulas para que Spark las interprete como SQL NULL
        for col_name in OUTPUT_COLUMNS:
            if col_name not in ["es_atipico"]:
                df_silver_final[col_name] = df_silver_final[col_name].astype(object)
                df_silver_final.loc[df_silver_final[col_name].isna(), col_name] = None

                df_rechazados_final[col_name] = df_rechazados_final[col_name].astype(object)
                df_rechazados_final.loc[df_rechazados_final[col_name].isna(), col_name] = None

        # 10. Spark: Conversión y escritura a HDFS con esquema explícito StructType
        print("[INFO] Iniciando SparkSession para persistencia Parquet en HDFS...")
        spark = (
            SparkSession.builder.appName("Osiptel-Conexiones-Silver")
            .master(os.getenv("SPARK_MASTER", "yarn"))
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )

        try:
            print(f"[INFO] Escribiendo Silver válido en Parquet particionado por anio: {HDFS_SILVER_PATH}")
            spark_silver = spark.createDataFrame(df_silver_final, schema=SCHEMA)
            spark_silver.write.mode("overwrite").format("parquet").option(
                "compression", "snappy"
            ).partitionBy("anio").save(HDFS_SILVER_PATH)

            print(f"[INFO] Escribiendo Cuarentena de rechazados en: {HDFS_RECHAZADOS_PATH}")
            spark_rechazados = spark.createDataFrame(df_rechazados_final, schema=SCHEMA)
            spark_rechazados.write.mode("overwrite").format("parquet").option(
                "compression", "snappy"
            ).save(HDFS_RECHAZADOS_PATH)

            # 11. Reporte de calidad dinámico
            reporte = _generar_reporte(
                total_filas=total_filas,
                rechazados_count=len(df_rechazados_final),
                validos_count=len(df_silver_final),
                duplicados_eliminados=duplicados_eliminados,
                conflictos_clave=conflictos_clave,
                atipicos_marcados=atipicos_marcados,
                nulos_por_columna=nulos_por_columna,
                completitud_por_campo=completitud_por_campo,
            )

            print(f"\n[ÉXITO] Reporte de calidad guardado en: {REPORT_PATH}")
            print(json.dumps(reporte, indent=2, ensure_ascii=False))
            return reporte

        finally:
            spark.stop()

    except Exception as exc:
        print(f"[ERROR] Falló el procesamiento del silver de conexiones: {exc}")
        raise
    finally:
        # Limpieza de archivo y directorio temporal
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    procesar_conexiones_a_silver()
