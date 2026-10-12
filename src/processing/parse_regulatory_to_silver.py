import glob
import json
import os
import re
import subprocess
from pathlib import Path
import pandas as pd

HDFS_MANDATOS_PARQUET = "hdfs://localhost:9000/data/silver/osiptel_mandatos_metricas.parquet"
HDFS_REPORTES_PARQUET = "hdfs://localhost:9000/data/silver/osiptel_reportes_metricas.parquet"
REPORT_REG_PATH = Path("evidence/reporte_regulatorio.json")


def extraer_metricas_mandatos() -> tuple:
    """Extrae atributos y métricas cuantitativas de los 142 mandatos de compartición en PDF."""
    try:
        import pypdf
    except ImportError:
        subprocess.run(["pip3", "install", "pypdf", "--quiet"], check=True)
        import pypdf

    candidatas = [
        "data/raw/datos/Osiptal_DatosNE*/*.pdf",
        "data/raw/Osiptal_DatosNE*/*.pdf",
        "data/raw/datos/osiptel_mandatos/*.pdf",
    ]
    archivos = []
    for c in candidatas:
        archivos = sorted(glob.glob(c))
        if archivos:
            break

    if not archivos:
        raise FileNotFoundError("No se encontraron los PDFs de mandatos de OSIPTEL.")

    records = []
    for f in archivos:
        fn = os.path.basename(f)
        m_anio = re.search(r"(20\d{2})", fn)
        anio = str(m_anio.group(1)) if m_anio else "2020"

        expediente = None
        try:
            reader = pypdf.PdfReader(f)
            p1 = reader.pages[0].extract_text() or ""
            m_exp = re.search(r"EXPEDIENTE\s*(?:N[º°\.]*)?\s*([0-9A-Z\-/]+)", p1, re.I)
            if m_exp:
                expediente = m_exp.group(1).strip()
        except Exception:
            pass

        records.append(
            {
                "archivo": fn,
                "anio": anio,
                "expediente": expediente or f"EXP-DPRC-{anio}",
                "materia": "COMPARTICION_INFRAESTRUCTURA_POSTES",
            }
        )

    df_detalle = pd.DataFrame(records)

    # Agregación por año para integración en Capa Gold
    df_metricas = (
        df_detalle.groupby("anio")
        .agg(
            mandatos_normativos_osiptel=("archivo", "count"),
            expedientes_distintos=("expediente", "nunique"),
        )
        .reset_index()
    )
    return df_detalle, df_metricas


def extraer_metricas_reportes() -> tuple:
    """Extrae atributos y métricas cuantitativas de los reportes estadísticos de mercado."""
    candidatas = [
        "data/raw/datos/ReportesOsiptel/*.pdf",
        "data/raw/ReportesOsiptel/*.pdf",
        "data/raw/datos/reportes_osiptel/*.pdf",
    ]
    archivos = []
    for c in candidatas:
        archivos = sorted(glob.glob(c))
        if archivos:
            break

    if not archivos:
        raise FileNotFoundError("No se encontraron los reportes estadísticos de OSIPTEL.")

    records = []
    for f in archivos:
        fn = os.path.basename(f)
        m_anio = re.search(r"(20\d{2})", fn)
        anio = str(m_anio.group(1)) if m_anio else "2019"

        fn_low = fn.lower()
        if "portabilidad" in fn_low:
            tema = "PORTABILIDAD_NUMERICA"
        elif "reclamo" in fn_low:
            tema = "RECLAMOS_USUARIOS"
        elif "antena" in fn_low or "infraestructura" in fn_low:
            tema = "DESPLIEGUE_ANTENAS"
        elif "4g" in fn_low or "lte" in fn_low:
            tema = "TECNOLOGIA_4G_LTE"
        elif "multa" in fn_low:
            tema = "SANCIONES_Y_MULTAS"
        elif "calidad" in fn_low:
            tema = "CALIDAD_DE_SERVICIO"
        else:
            tema = "SUPERVISION_MERCADO_TELECOM"

        records.append(
            {
                "archivo": fn,
                "anio": anio,
                "tema_supervision": tema,
            }
        )

    df_detalle = pd.DataFrame(records)

    # Agregación por año para integración en Capa Gold
    df_metricas = (
        df_detalle.groupby("anio")
        .agg(
            reportes_supervision_osiptel=("archivo", "count"),
            temas_supervisados_distintos=("tema_supervision", "nunique"),
        )
        .reset_index()
    )
    return df_detalle, df_metricas


def process_regulatory_silver(forzar: bool = False):
    """Punto de entrada: procesa mandatos y reportes a métricas Silver estructuradas."""
    print("=== PROCESAMIENTO SILVER: MANDATOS Y REPORTES REGULATORIOS (PDF -> METRICAS) ===")

    mand_detalle_path = Path("data/silver/osiptel_mandatos_detalle.parquet")
    mand_metricas_path = Path("data/silver/osiptel_mandatos_metricas.parquet")
    rep_detalle_path = Path("data/silver/osiptel_reportes_detalle.parquet")
    rep_metricas_path = Path("data/silver/osiptel_reportes_metricas.parquet")

    if (
        not forzar
        and mand_metricas_path.exists()
        and rep_metricas_path.exists()
        and REPORT_REG_PATH.exists()
    ):
        print("[INFO] Métricas regulatorias Silver ya existen. Se omite por persistencia.")
        with open(REPORT_REG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    # 1. Mandatos
    df_mand_det, df_mand_met = extraer_metricas_mandatos()
    Path("data/silver").mkdir(parents=True, exist_ok=True)
    df_mand_det.to_parquet(mand_detalle_path, index=False)
    df_mand_met.to_parquet(mand_metricas_path, index=False)
    print(f"[INFO] Mandatos procesados: {len(df_mand_det)} documentos -> {len(df_mand_met)} años métricos.")

    # 2. Reportes
    df_rep_det, df_rep_met = extraer_metricas_reportes()
    df_rep_det.to_parquet(rep_detalle_path, index=False)
    df_rep_met.to_parquet(rep_metricas_path, index=False)
    print(f"[INFO] Reportes procesados: {len(df_rep_det)} documentos -> {len(df_rep_met)} años métricos.")

    # 3. Auditoría
    reporte = {
        "total_mandatos_pdf": len(df_mand_det),
        "anios_mandatos_cubiertos": df_mand_met["anio"].tolist(),
        "total_reportes_pdf": len(df_rep_det),
        "anios_reportes_cubiertos": df_rep_met["anio"].tolist(),
        "archivos_silver_generados": [
            str(mand_metricas_path),
            str(mand_detalle_path),
            str(rep_metricas_path),
            str(rep_detalle_path),
        ],
    }

    REPORT_REG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_REG_PATH, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print(f"[ÉXITO] Reporte de auditoría regulatoria guardado en: {REPORT_REG_PATH}")
    return reporte


if __name__ == "__main__":
    process_regulatory_silver(forzar=True)
