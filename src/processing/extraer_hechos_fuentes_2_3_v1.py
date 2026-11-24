#!/usr/bin/env python3
"""OSIPTEL: extracción conservadora de hechos estructurados, NO Silver definitivo.

Lee PDF originales en staging local (mismos bytes que Bronze verificado) y genera
hechos regulatorios y cifras estadísticas candidatas con referencia de página.
No transforma menciones en decisiones concedidas; no interpreta porcentajes
sin contexto y no crea ni modifica rutas HDFS.
Compatible con Python 3.8; requiere pdftotext de Poppler.
"""
import csv
import hashlib
import json
import re
import subprocess
import unicodedata
from collections import Counter
from pathlib import Path

BASE = Path('/srv/osiptel/staging/extraido_da/da')
OUT = Path('/srv/osiptel/evidence/hechos_pdf_2_3_v1')
OUT.mkdir(parents=True, exist_ok=True)

REGLAS_REGULATORIAS = [
    ('EVALUACION_SOLICITUD', r'\bevaluacion de (?:la )?solicitud de mandato\b'),
    ('PROYECTO_MANDATO', r'\bproyecto de mandato de (?:comparticion|acceso)\b'),
    ('SOLICITUD_MANDATO', r'\bsolicitud de mandato de (?:comparticion|acceso)\b'),
    ('INFRAESTRUCTURA_COMPARTIDA', r'\bcomparticion de infraestructura(?: electrica)?\b'),
    ('CONDICION_ECONOMICA_MENCIONADA', r'\bcontraprestacion\b|\bcondiciones economicas\b'),
]

# Exigir simultáneamente número, unidad "conexiones" y el objeto "internet fijo".
# No confundir "otras tecnologías" con fibra óptica, ni "internet fijo" en una
# noticia de portabilidad móvil con una estadística del mercado fijo.
NUMERO = r'(?:\d{1,3}(?:[ \u00a0.,]\d{3})+|\d{4,})'
PATRON_CANTIDAD = re.compile(
    r'(?<!\w)(?P<cantidad>' + NUMERO + r')\s+conexiones\s+'
    r'(?:(?:de|del)\s+|al\s+servicio\s+de\s+)?'
    r'(?P<servicio>internet\s+fijo|banda\s+ancha\s+fija)\b',
    re.I
)
PATRON_FIJO = re.compile(r'\binternet fijo\b|\bbanda ancha fija\b|\bfibra optica\b', re.I)
PATRON_DEPARTAMENTO_VARIACION = re.compile(
    r'\b(?P<departamento>HUANCAVELICA|ICA|APURIMAC|LAMBAYEQUE|AMAZONAS|'
    r'ANCASH|AREQUIPA|AYACUCHO|CAJAMARCA|CALLAO|CUSCO|HUANUCO|JUNIN|'
    r'LA LIBERTAD|LIMA|LORETO|MADRE DE DIOS|MOQUEGUA|PASCO|PIURA|PUNO|'
    r'SAN MARTIN|TACNA|TUMBES|UCAYALI)\s*\(\s*\+?'
    r'(?P<porcentaje>\d{1,3}(?:[.,]\d{1,2})?)\s*%\s*\)', re.I
)


def normalizar(texto):
    t = unicodedata.normalize('NFKD', texto)
    return ''.join(c for c in t if not unicodedata.combining(c)).upper()


def contexto(lineas, indice, antes=2, despues=2):
    lo = max(0, indice - antes)
    hi = min(len(lineas), indice + despues + 1)
    return ' '.join(x.strip() for x in lineas[lo:hi] if x.strip())[:650]


def guardar_csv(ruta, encabezado, filas):
    with ruta.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(encabezado)
        w.writerows(filas)


hechos_reg = []
hechos_est = []
auditoria = []
metricas = Counter()

for fuente, subcarpeta in [('FUENTE_2', 'Fuente 2'), ('FUENTE_3', 'Fuente 3')]:
    rutas = sorted((BASE / subcarpeta).rglob('*.pdf'))
    print('Analizando', fuente, 'PDF:', len(rutas), flush=True)

    for archivo in rutas:
        sha = hashlib.sha256(archivo.read_bytes()).hexdigest()
        etiquetas = Counter()
        paginas_analizadas = 0
        error = ''
        vistas = set()
        referencia_fijo = False
        en_documento_movil = bool(re.search(
            r'portabilidad|trafico.*movil|telefonia.movil|internet.movil',
            normalizar(archivo.name), re.I
        ))

        try:
            r = subprocess.run(
                ['pdftotext', '-layout', str(archivo), '-'],
                capture_output=True, text=True, errors='replace', timeout=120
            )
            if r.returncode:
                raise RuntimeError(r.stderr[:200])

            # Preservar páginas vacías internas: suprimir sólo el \f final.
            paginas = r.stdout.split('\f')
            if paginas and not paginas[-1].strip():
                paginas.pop()
            paginas_analizadas = len(paginas)

            for num_pag, contenido in enumerate(paginas, 1):
                lineas = contenido.splitlines()
                pagina_norm = normalizar(' '.join(lineas))

                if fuente == 'FUENTE_2':
                    # Conservar como máximo un ejemplo por (documento, regla).
                    for regla, patron in REGLAS_REGULATORIAS:
                        if etiquetas[regla]:
                            continue
                        for i, linea in enumerate(lineas):
                            ventana = contexto(lineas, i, antes=2, despues=2)
                            if re.search(patron, normalizar(ventana), re.I):
                                etiquetas[regla] += 1
                                hechos_reg.append([
                                    archivo.name, sha, num_pag, i + 1, regla,
                                    'MENCION_DOCUMENTAL_NO_EQUIVALE_A_MANDATO_APROBADO',
                                    ventana
                                ])
                                break
                else:
                    if PATRON_FIJO.search(pagina_norm):
                        referencia_fijo = True
                    # Buscar cifras exactas una vez por página para no
                    # triplicar resultados cuando una ventana solapa líneas.
                    texto_plano = ' '.join(x.strip() for x in lineas if x.strip())
                    for m in PATRON_CANTIDAD.finditer(texto_plano):
                        bruto = re.sub(r'[ \u00a0.,]', '', m.group('cantidad'))
                        if not bruto.isdigit():
                            continue
                        numero = int(bruto)
                        if numero <= 0:
                            continue
                        clave = (num_pag, 'CONEXIONES_FIJAS', m.start(), numero)
                        if clave in vistas:
                            continue
                        vistas.add(clave)
                        etiquetas['CANTIDAD_FIJA_EXPLICITA'] += 1
                        fragmento = texto_plano[max(0, m.start()-180):m.end()+180][:650]
                        hechos_est.append([
                            archivo.name, sha, num_pag, '',
                            'CONEXIONES_FIJAS_EXPLICITAS', str(numero), 'conexiones',
                            '', '',
                            'REVISAR_DOCUMENTO_MOVIL' if en_documento_movil
                            else 'CANDIDATO_PERIODO_Y_AMBITO_NO_VERIFICADOS',
                            fragmento
                        ])

                    # Variación departamental expresada explícitamente dentro de
                    # sección que nombra conexiones de internet fijo. Es un
                    # candidato, nunca un crecimiento anual confirmado.
                    if re.search(r'EVOLUCION DE LAS CONEXIONES (?:DE|AL SERVICIO DE) INTERNET FIJO POR DEPARTAMENTO', pagina_norm):
                        texto_plano = ' '.join(lineas)
                        for m in PATRON_DEPARTAMENTO_VARIACION.finditer(normalizar(texto_plano)):
                            departamento = m.group('departamento').title()
                            valor = m.group('porcentaje').replace(',', '.')
                            if not 0 <= float(valor) <= 100:
                                continue
                            clave = (num_pag, 'VARIACION', departamento, valor)
                            if clave in vistas:
                                continue
                            vistas.add(clave)
                            etiquetas['VARIACION_DEPARTAMENTO_CANDIDATA'] += 1
                            hechos_est.append([
                                archivo.name, sha, num_pag, '',
                                'VARIACION_DEPARTAMENTAL_REPORTADA', valor,
                                'porcentaje', departamento, '',
                                'CANDIDATO_SIN_BASE_DE_COMPARACION_VERIFICADA',
                                texto_plano[max(0, m.start()-180):m.end()+180][:650]
                            ])
        except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
            error = str(exc)[:250]

        n_hechos = sum(etiquetas.values())
        if error:
            estado = 'ERROR_LECTURA'
        elif n_hechos:
            estado = 'HECHOS_CANDIDATOS'
        elif fuente == 'FUENTE_3' and referencia_fijo:
            estado = 'REFERENCIAS_SIN_VALOR_ESTRICTO'
        else:
            estado = 'SIN_HECHOS_ESTRICTOS'
        auditoria.append([fuente, archivo.name, sha, paginas_analizadas,
                          n_hechos, estado, error])
        metricas[(fuente, estado)] += 1

salida_reg = OUT / 'fuente2_hechos_regulatorios_candidatos.csv'
salida_est = OUT / 'fuente3_indicadores_estadisticos_candidatos.csv'
guardar_csv(salida_reg, ['documento', 'sha256', 'pagina_pdf', 'linea_pdf',
                         'tipo_hecho', 'estado_validacion', 'evidencia'], hechos_reg)
guardar_csv(salida_est, ['documento', 'sha256', 'pagina_pdf', 'linea_pdf',
                         'indicador_candidato', 'valor', 'unidad', 'ambito_mencionado',
                         'periodo_verificado', 'estado_validacion', 'evidencia'], hechos_est)
guardar_csv(OUT / 'auditoria.csv', ['fuente', 'documento', 'sha256', 'paginas',
                                    'registros_candidatos', 'estado', 'error'], auditoria)
resumen = {
    'pdf_analizados': len(auditoria),
    'hechos_regulatorios_candidatos': len(hechos_reg),
    'indicadores_estadisticos_candidatos': len(hechos_est),
    'por_fuente_estado': {f'{f}_{e}': n for (f, e), n in sorted(metricas.items())},
    'estado': 'ESTRUCTURACION_EXPERIMENTAL_NO_ES_SILVER_VALIDADO',
    'advertencias': [
        'Fuente 2: solicitud, evaluación o proyecto NO implica mandato concedido',
        'Fuente 3: periodos y bases comparativas NO se infieren del nombre del PDF',
        'Filas numéricas requieren contraste contra tabla/página original',
    ]
}
(OUT / 'resumen.json').write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding='utf-8')
print('\n=== RESUMEN ESTRUCTURACION PDF ===')
print(json.dumps(resumen, indent=2, ensure_ascii=False))
print('Salida:', OUT)
