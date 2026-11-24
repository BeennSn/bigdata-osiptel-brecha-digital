from pathlib import Path
from collections import Counter
import subprocess
import hashlib
import csv
import json
import re

BASE = Path("/srv/osiptel/staging/extraido_da/da")
SALIDA = Path("/srv/osiptel/evidence/extraccion_pdf_2_3")
SALIDA.mkdir(parents=True, exist_ok=True)

FUENTES = {
    "FUENTE_2": {
        "carpeta": BASE / "Fuente 2",
        "patrones": {
            "MANDATO_ACCESO": r"\bmandato (?:de |para )?(?:compartici[oó]n|acceso)\b",
            "COMPARTICION": r"\bcompartici[oó]n de infraestructura\b",
            "INFRAESTRUCTURA_ELECTRICA": r"\binfraestructura el[eé]ctrica\b",
            "POSTES_DUCTOS": r"\bpostes?\b|\bductos?\b",
            "CONDICIONES_ECONOMICAS": r"\bcontraprestaci[oó]n\b|\bcondiciones econ[oó]micas\b",
        }
    },
    "FUENTE_3": {
        "carpeta": BASE / "Fuente 3",
        "patrones": {
            "CONEXIONES_FIJAS": r"\bconexiones? (?:de )?(?:internet fijo|banda ancha fija)\b",
            "INTERNET_FIJO": r"\binternet fijo\b|\bbanda ancha fija\b",
            "FIBRA_OPTICA": r"\bfibra [oó]ptica\b|\bftth\b",
            "VELOCIDAD_FIJA": r"\bvelocidad(?:es)? de (?:internet fijo|bajada|descarga)\b",
        }
    }
}

evidencias = []
auditoria = []
resumen = Counter()

for fuente, config in FUENTES.items():
    archivos = sorted(config["carpeta"].rglob("*.pdf"))

    print("Procesando", fuente, "|", len(archivos), "PDF", flush=True)

    for archivo in archivos:
        sha = hashlib.sha256(archivo.read_bytes()).hexdigest()
        encontrados = Counter()
        paginas_analizadas = 0
        error = ""

        try:
            resultado = subprocess.run(
                ["pdftotext", "-layout", str(archivo), "-"],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=120
            )

            if resultado.returncode != 0:
                raise RuntimeError(resultado.stderr[:200])

            paginas = [
                p for p in resultado.stdout.split("\f")
                if p.strip()
            ]

            paginas_analizadas = len(paginas)

            for num_pagina, contenido in enumerate(paginas, 1):
                lineas = contenido.splitlines()

                for num_linea, linea in enumerate(lineas, 1):
                    if not linea.strip():
                        continue

                    for categoria, patron in config["patrones"].items():
                        # Máximo 3 fragmentos por categoría y PDF.
                        if encontrados[categoria] >= 3:
                            continue

                        if not re.search(patron, linea, re.I):
                            continue

                        inicio = max(0, num_linea - 2)
                        fin = min(len(lineas), num_linea + 2)

                        fragmento = " ".join(
                            x.strip() for x in lineas[inicio:fin]
                            if x.strip()
                        )[:500]

                        cifras = re.findall(
                            r"(?<!\w)\d[\d.,]*\s*%?",
                            fragmento
                        )[:10]

                        evidencias.append([
                            fuente,
                            archivo.name,
                            sha,
                            num_pagina,
                            num_linea,
                            categoria,
                            fragmento,
                            ";".join(cifras)
                        ])

                        encontrados[categoria] += 1

        except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
            error = str(exc)[:250]

        cantidad = sum(encontrados.values())

        if error:
            estado = "ERROR_LECTURA"
        elif cantidad:
            estado = "EVIDENCIA_CANDIDATA"
        else:
            estado = "SIN_EVIDENCIA_DETECTADA"

        resumen[(fuente, estado)] += 1

        auditoria.append([
            fuente,
            archivo.name,
            sha,
            paginas_analizadas,
            cantidad,
            estado,
            error
        ])

with (SALIDA / "evidencias.csv").open(
    "w", newline="", encoding="utf-8"
) as f:
    w = csv.writer(f)
    w.writerow([
        "fuente", "documento", "sha256", "pagina_pdf",
        "linea_pdf", "categoria", "fragmento",
        "cifras_candidatas_no_validadas"
    ])
    w.writerows(evidencias)

with (SALIDA / "auditoria.csv").open(
    "w", newline="", encoding="utf-8"
) as f:
    w = csv.writer(f)
    w.writerow([
        "fuente", "documento", "sha256", "paginas",
        "evidencias", "estado", "error"
    ])
    w.writerows(auditoria)

reporte = {
    "pdf_analizados": len(auditoria),
    "fragmentos_candidatos": len(evidencias),
    "por_fuente_estado": {
        f"{fuente}_{estado}": cantidad
        for (fuente, estado), cantidad in sorted(resumen.items())
    },
    "estado": "EXTRACCION_PRELIMINAR_NO_PUBLICADA_EN_SILVER"
}

(SALIDA / "resumen.json").write_text(
    json.dumps(reporte, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("\n=== RESULTADO EXTRACCION PDF ===")
print(json.dumps(reporte, indent=2, ensure_ascii=False))
print("Archivos:", SALIDA)
