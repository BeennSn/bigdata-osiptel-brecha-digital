from pathlib import Path
from zipfile import ZipFile
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import xml.etree.ElementTree as ET
import hashlib
import csv
import json
import re

ORIGEN = Path(
    "/srv/osiptel/staging/extraido_da/da/Fuente 1/"
    "2.1 CONEXIONES DE INTERNET FIJO.xlsx"
)

DESTINO = Path(
    "/srv/osiptel/staging/silver_input/"
    "conexiones_fuente1.csv"
)

REPORTE = Path(
    "/srv/osiptel/evidence/"
    "preparacion_conexiones_silver.json"
)

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

CAMPOS = {
    "B": "mes_excel",
    "C": "empresa",
    "D": "tecnologia",
    "E": "segmento",
    "F": "departamento",
    "G": "rango_velocidad",
    "H": "conexiones_original"
}

DESTINO.parent.mkdir(parents=True, exist_ok=True)
REPORTE.parent.mkdir(parents=True, exist_ok=True)

sha = hashlib.sha256(ORIGEN.read_bytes()).hexdigest()

total = 0
suma_conexiones = 0
rechazados = 0
vacios = Counter()
meses = Counter()

with ZipFile(ORIGEN) as z:
    cadenas = []

    if "xl/sharedStrings.xml" in z.namelist():
        raiz = ET.fromstring(z.read("xl/sharedStrings.xml"))
        for si in raiz.findall(NS + "si"):
            cadenas.append("".join(
                t.text or "" for t in si.iter(NS + "t")
            ))

    with DESTINO.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)

        escritor.writerow([
            "fila_excel",
            "mes_excel",
            "mes",
            "empresa",
            "tecnologia",
            "segmento",
            "departamento",
            "rango_velocidad",
            "conexiones_original",
            "conexiones",
            "estado_validacion",
            "observaciones",
            "archivo_origen",
            "sha256_origen"
        ])

        with z.open("xl/worksheets/sheet1.xml") as archivo:
            for _, fila in ET.iterparse(archivo, events=("end",)):
                if fila.tag != NS + "row":
                    continue

                numero_fila = int(fila.attrib.get("r", "0"))

                if numero_fila <= 4:
                    fila.clear()
                    continue

                datos = {}

                for celda in fila.findall(NS + "c"):
                    referencia = celda.attrib.get("r", "")
                    coincidencia = re.match(r"[A-Z]+", referencia)

                    if not coincidencia:
                        continue

                    columna = coincidencia.group()

                    if columna not in CAMPOS:
                        continue

                    valor = celda.find(NS + "v")
                    texto = valor.text if valor is not None else ""

                    if celda.attrib.get("t") == "s" and texto:
                        texto = cadenas[int(texto)]
                    elif celda.attrib.get("t") == "inlineStr":
                        texto = "".join(
                            t.text or "" for t in celda.iter(NS + "t")
                        )

                    datos[columna] = str(texto).strip()

                if not datos.get("B") and not datos.get("H"):
                    fila.clear()
                    continue

                total += 1
                observaciones = []

                for columna, nombre in CAMPOS.items():
                    if not datos.get(columna):
                        vacios[nombre] += 1
                        observaciones.append("VACIO_" + nombre.upper())

                mes = ""
                conexiones = ""

                try:
                    fecha = datetime(1899, 12, 30) + timedelta(
                        days=float(datos["B"])
                    )
                    mes = fecha.strftime("%Y-%m-%d")
                    meses[fecha.strftime("%Y-%m")] += 1
                except (ValueError, KeyError, OverflowError):
                    observaciones.append("MES_INVALIDO")

                try:
                    numero = Decimal(datos["H"])

                    if numero < 0 or numero != numero.to_integral_value():
                        raise ValueError()

                    conexiones = int(numero)
                    suma_conexiones += conexiones

                except (InvalidOperation, ValueError, KeyError):
                    observaciones.append("CONEXIONES_INVALIDAS")

                if not mes or conexiones == "":
                    estado = "RECHAZADO"
                    rechazados += 1
                elif observaciones:
                    estado = "OBSERVADO"
                else:
                    estado = "VALIDO"

                escritor.writerow([
                    numero_fila,
                    datos.get("B", ""),
                    mes,
                    datos.get("C", ""),
                    datos.get("D", ""),
                    datos.get("E", ""),
                    datos.get("F", ""),
                    datos.get("G", ""),
                    datos.get("H", ""),
                    conexiones,
                    estado,
                    ";".join(observaciones),
                    ORIGEN.name,
                    sha
                ])

                fila.clear()

resumen = {
    "archivo_origen": ORIGEN.name,
    "sha256_origen": sha,
    "filas_procesadas": total,
    "filas_rechazadas": rechazados,
    "suma_conexiones_filas_validas": suma_conexiones,
    "periodos": len(meses),
    "primer_mes": min(meses) if meses else None,
    "ultimo_mes": max(meses) if meses else None,
    "campos_vacios": dict(vacios),
    "archivo_preparado": str(DESTINO),
    "estado": "PREPARACION_LOCAL_NO_PUBLICADA_EN_SILVER"
}

REPORTE.write_text(
    json.dumps(resumen, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print("=== PREPARACION FUENTE 1 ===")
print(json.dumps(resumen, indent=2, ensure_ascii=False))
