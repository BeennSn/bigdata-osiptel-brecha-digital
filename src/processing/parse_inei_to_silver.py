import json
import os
import re
import subprocess
import unicodedata
from pathlib import Path
import pandas as pd

HDFS_SILVER_INEI = "hdfs://localhost:9000/data/silver/inei_indicadores.parquet"
LOCAL_SILVER_DIR = Path("data/silver")
LOCAL_SILVER_FILE = LOCAL_SILVER_DIR / "inei_indicadores.parquet"
REPORT_INEI_PATH = Path("evidence/reporte_inei.json")

# Archivos clave que concentran la serie histórica trimestral oficial de ENAHO
ARCHIVOS_CLAVE_INEI = [
    "01-informe-tecnico-tic-iv-trimestre-2020.pdf",
    "01-informe-tecnico-tic-iv-trimestre-2022.pdf",
    "03-informe-tecnico-tics-ii-trimestre-2023.pdf",
    "informe-tecnico_tics_oct-nov-dic24.pdf",
]

NUMERO = re.compile(r"(?<!\d)\d{1,3},\d(?!\d)")
TRIMESTRE = re.compile(
    r"^\s*(Ene-Feb-Mar|Abr-May-Jun|Jul-Ago-Se(?:p(?:t)?|t)|Oct-Nov-Dic)\b", re.I
)
TRIM_MAP = {"ene": 1, "abr": 2, "jul": 3, "oct": 4}


def _hdfs_path_existe(ruta_hdfs: str) -> bool:
    try:
        res = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", ruta_hdfs],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return res.returncode == 0
    except Exception:
        return False


def extraer_indicadores_inei() -> pd.DataFrame:
    """Extrae las series trimestrales de los boletines oficiales en PDF del INEI."""
    try:
        import pypdf
    except ImportError:
        subprocess.run(["pip3", "install", "pypdf", "--quiet"], check=True)
        import pypdf

    # Buscar carpeta de origen
    candidatas_dir = [
        Path("data/raw/datos/DatasetINEI"),
        Path("data/raw/DatasetINEI"),
        Path("data/raw/datos/dataset_inei"),
    ]
    dir_inei = next((d for d in candidatas_dir if d.exists()), None)
    if not dir_inei:
        raise FileNotFoundError("No se encontró la carpeta con PDFs del INEI (DatasetINEI).")

    registros = []
    for nombre_pdf in ARCHIVOS_CLAVE_INEI:
        ruta_pdf = dir_inei / nombre_pdf
        if not ruta_pdf.exists():
            continue

        try:
            reader = pypdf.PdfReader(str(ruta_pdf))
            for num_pag, page in enumerate(reader.pages, 1):
                txt = page.extract_text() or ""
                if (
                    ("Cuadro Nº 1" in txt or "Cuadro N° 1" in txt)
                    and "Hogares con acceso a servicios" in txt
                    and "Internet" in txt
                ):
                    lines = txt.splitlines()
                    seccion, anio = "", None
                    for linea in lines:
                        if "Indicadores trimestrales" in linea:
                            seccion = "trimestral"
                            continue
                        if (
                            "Variación" in linea
                            or "Variacion" in linea
                            or "Fuente:" in linea
                        ):
                            break
                        if seccion == "trimestral":
                            m_anio = re.match(r"^\s*(20\d{2})(?:\s+P/)?\s*$", linea)
                            if m_anio:
                                anio = int(m_anio.group(1))
                                continue
                            m_trim = TRIMESTRE.match(linea)
                            if m_trim and anio:
                                nums = [
                                    float(x.replace(",", "."))
                                    for x in NUMERO.findall(linea[m_trim.end() :])
                                ]
                                if len(nums) == 5:
                                    pfx = m_trim.group(1)[:3].lower()
                                    registros.append(
                                        {
                                            "anio": str(anio),
                                            "trimestre": int(TRIM_MAP[pfx]),
                                            "telefonia_fija_pct": nums[0],
                                            "telefonia_movil_pct": nums[1],
                                            "tv_cable_pct": nums[2],
                                            "computadora_pct": nums[3],
                                            "internet_pct": nums[4],
                                            "fuente_pdf": nombre_pdf,
                                            "pagina_origen": num_pag,
                                        }
                                    )
        except Exception as e:
            print(f"[ADVERTENCIA] Error leyendo {nombre_pdf}: {e}")

    df = pd.DataFrame(registros)
    if df.empty:
        raise ValueError("No se pudieron extraer indicadores trimestrales de los PDFs de INEI.")

    # Conservar la versión más reciente por cada periodo trimestral
    df_dedup = (
        df.drop_duplicates(subset=["anio", "trimestre"], keep="last")
        .sort_values(by=["anio", "trimestre"])
        .reset_index(drop=True)
    )
    return df_dedup


def process_inei_silver(forzar: bool = False):
    """Punto de entrada estándar: procesa PDFs del INEI a Capa Silver en Parquet y HDFS."""
    print("=== INICIANDO PROCESAMIENTO SILVER: INDICADORES INEI (PDF -> PARQUET) ===")

    if not forzar and LOCAL_SILVER_FILE.exists() and REPORT_INEI_PATH.exists():
        print(f"[INFO] Capa Silver de INEI ya existe en {LOCAL_SILVER_FILE}. Se omite por persistencia.")
        with open(REPORT_INEI_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    df_inei = extraer_indicadores_inei()
    print(f"[INFO] Indicadores INEI extraídos exitosamente: {len(df_inei)} trimestres.")

    # 1. Guardar localmente
    LOCAL_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df_inei.to_parquet(LOCAL_SILVER_FILE, index=False)
    print(f"[INFO] Guardado en almacenamiento local: {LOCAL_SILVER_FILE}")

    # 2. Subir a HDFS si está disponible
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data/silver/"], check=False)
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", str(LOCAL_SILVER_FILE), HDFS_SILVER_INEI],
            check=False,
        )
        print(f"[INFO] Capa Silver de INEI sincronizada en HDFS: {HDFS_SILVER_INEI}")
    except Exception as e:
        print(f"[ADVERTENCIA] No se pudo subir a HDFS ({e}). Se conserva copia local.")

    # 3. Reporte de auditoría
    reporte = {
        "origen": "DatasetINEI (Informes Técnicos ENAHO)",
        "destino_local": str(LOCAL_SILVER_FILE),
        "destino_hdfs": HDFS_SILVER_INEI,
        "total_trimestres_extraidos": len(df_inei),
        "anio_minimo": df_inei["anio"].min(),
        "anio_maximo": df_inei["anio"].max(),
        "indicadores_extraidos": [
            "internet_pct",
            "computadora_pct",
            "telefonia_fija_pct",
            "telefonia_movil_pct",
            "tv_cable_pct",
        ],
    }

    REPORT_INEI_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_INEI_PATH, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print(f"[ÉXITO] Reporte de calidad INEI guardado en: {REPORT_INEI_PATH}")
    return reporte


if __name__ == "__main__":
    process_inei_silver(forzar=True)
