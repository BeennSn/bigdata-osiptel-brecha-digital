#!/usr/bin/env python3
"""Extraccion experimental auditable de acceso de hogares a Internet (INEI).

Compatible con Python 3.8 y Poppler pdftotext; NO escribe a HDFS/Silver.
Separa evidencia automatica, conflictos y consolidado sin conflictos.
"""
import argparse
import csv
import hashlib
import json
import re
import subprocess
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

ORIGEN = Path('/srv/osiptel/staging/extraido_da/da/Fuente 4')
DESTINO = Path('/srv/osiptel/evidence/inei_fuente4_piloto')
AREAS = ('Nacional', 'Lima Metropolitana', 'Resto urbano', 'Area rural')
CAMPOS = ('periodo', 'ambito', 'porcentaje', 'indicador', 'unidad', 'documento',
          'sha256_pdf', 'pagina_pdf', 'referencia', 'metodo', 'evidencia')


def normalizar(s):
    s = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in s if not unicodedata.combining(c))


def compacto(s):
    return re.sub(r'\s+', ' ', normalizar(s)).strip()


def num(s):
    v = float(s.replace(',', '.'))
    if not 0 <= v <= 100:
        raise ValueError('Porcentaje fuera de rango: ' + str(v))
    return v


def sha256_archivo(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def indicador(periodo, ambito, valor, nombre, sha, pagina, referencia, metodo, evidencia):
    return dict(periodo=periodo, ambito=ambito, porcentaje=valor,
                indicador='Hogares con acceso a Internet', unidad='porcentaje',
                documento=nombre, sha256_pdf=sha, pagina_pdf=pagina,
                referencia=referencia, metodo=metodo,
                evidencia=evidencia.strip()[:1000])


def extraer_narracion(pagina, n_pagina, nombre, sha):
    texto = compacto(pagina)
    # Patrón deliberadamente acotado: evita confundir uso de internet
    # de personas, conectividad móvil, o tenencia general de TIC.
    patron = re.compile(
        r'Durante el (primer|segundo|tercer|cuarto) trimestre de (20\d{2}),'
        r'\s*el\s*(\d{1,3}[,.]\d{1,2})\s*%\s*de los hogares del pais'
        r'\s*contaba con servicio de Internet', re.I)
    trimestres = dict(primer=1, segundo=2, tercer=3, cuarto=4)
    lista = []
    for m in patron.finditer(texto):
        if '1.2.3' not in texto and 'hogares con computadora' not in texto.lower():
            continue
        seg = texto[m.end():m.end() + 450]
        expresiones = {
            'Lima Metropolitana': r'en Lima Metropolitana,\s*el\s*(\d{1,3}[,.]\d{1,2})%',
            'Resto urbano': r'en el resto urbano,\s*el\s*(\d{1,3}[,.]\d{1,2})%',
            'Area rural': r'en el area rural\s+solo el\s*(\d{1,3}[,.]\d{1,2})%',
        }
        valores = {'Nacional': num(m.group(3))}
        for area, p in expresiones.items():
            a = re.search(p, seg, re.I)
            if a:
                valores[area] = num(a.group(1))
        # Estrategia narrativa: exigir los cuatro valores en una misma sección
        if len(valores) != 4:
            continue
        periodo = '{}-T{}'.format(m.group(2), trimestres[m.group(1).lower()])
        evidencia = texto[m.start():m.end()] + ' ' + seg[:280]
        for area in AREAS:
            lista.append(indicador(periodo, area, valores[area], nombre, sha,
                                   n_pagina, 'Seccion 1.2.3', 'NARRATIVA', evidencia))
    return lista


def trimestre(encabezado):
    t = compacto(encabezado).lower()
    # No inventar trimestre a partir del nombre del PDF.
    if re.search(r'oct\w*\s*[-– ]\s*nov\w*\s*[-– ]\s*dic\w*', t):
        return 4
    if re.search(r'ene\w*\s*[-– ]\s*feb\w*\s*[-– ]\s*mar\w*', t):
        return 1
    if re.search(r'abr\w*\s*[-– ]\s*may\w*\s*[-– ]\s*jun\w*', t):
        return 2
    if re.search(r'jul\w*\s*[-– ]\s*ago\w*\s*[-– ]\s*se[tp]\w*', t):
        return 3
    # También cabeceras abreviadas de 'I Trimestre'.
    m = re.search(r'\b([IiVvXx]{1,3})\s+trimestre\b', t)
    if m:
        return {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4}.get(m.group(1))
    return None


def extraer_tabla(pagina, n_pagina, nombre, sha):
    lineas = pagina.splitlines()
    lineas_n = [compacto(x) for x in lineas]
    lista = []
    for idx, titulo in enumerate(lineas_n):
        if not re.search(r'Peru:\s*Hogares con (?:acceso a\s+)?[Cc]omputadora\s+e\s+Internet,\s+segun area de residencia', titulo, re.I):
            continue
        # Delimitar tabla; cabeceras y filas deben estar cerca del título.
        sub = lineas_n[idx:idx + 23]
        trimestre_linea = next((x for x in sub[:7] if 'Trimestre:' in x), '')
        if not trimestre_linea:
            continue
        trim = trimestre(trimestre_linea)
        anios = re.findall(r'\b20\d{2}\b', trimestre_linea)
        if not trim or not (2 <= len(anios) <= 8) or len(set(anios)) != len(anios):
            continue
        filas = {}
        pat = re.compile(r'^(Total|Lima Metropolitana(?:\s+[12]/)?|Resto urbano(?:\s+[12]/)?|Area rural)\s+(.*)$', re.I)
        for row in sub[2:]:
            m = pat.match(row)
            if not m:
                continue
            area = re.sub(r'\s+[12]/$', '', m.group(1), flags=re.I)
            area = 'Nacional' if area.lower() == 'total' else area
            area = next((x for x in AREAS if x.lower() == area.lower()), area)
            numeros = re.findall(r'(?<!\d)-?\d{1,3},\d{1,2}(?!\d)', m.group(2))
            if len(numeros) >= 2 * len(anios):
                filas[area] = (numeros, row)
        # Una tabla parcial queda sin publicar, no rellenar ámbitos ausentes.
        if set(filas) != set(AREAS):
            continue
        # El formato comprobado alterna Computadora e Internet por año.
        # No aplicar si el encabezado no muestra ambas variables.
        encabezado = ' '.join(sub[:13]).lower()
        if not ('computadora' in encabezado and 'internet' in encabezado):
            continue
        referencia = 'Tabla/cuadro de Internet por area (revisar numero)'
        # Buscar el numero del cuadro en lineas que preceden al título.
        anterior = ' '.join(lineas_n[max(0, idx-3):idx])
        nro = re.search(r'Cuadro\s+N[^\d]{0,6}(\d+\.\d+)', anterior, re.I)
        if nro:
            referencia = 'Cuadro {}'.format(nro.group(1))
        for area in AREAS:
            numeros, evidencia = filas[area]
            for j, anio in enumerate(anios):
                valor = num(numeros[2*j + 1])
                lista.append(indicador('{}-T{}'.format(anio, trim), area, valor,
                                       nombre, sha, n_pagina, referencia,
                                       'TABLA_PAREADA', evidencia))
    return lista


def guardar_csv(ruta, filas, campos):
    with ruta.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction='ignore')
        w.writeheader()
        w.writerows(filas)


def ejecutar(base, salida):
    if not base.is_dir():
        raise RuntimeError('No existe la carpeta: ' + str(base))
    salida.mkdir(parents=True, exist_ok=True)
    documentos = sorted(base.rglob('*.pdf'))
    hallazgos, auditoria = [], []
    for i, pdf in enumerate(documentos, 1):
        registro = dict(documento=pdf.name, sha256_pdf='', paginas=0,
                        indicadores_detectados=0, estado='', detalle='')
        try:
            proceso = subprocess.run(['pdftotext', '-layout', str(pdf), '-'],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     universal_newlines=True, errors='replace',
                                     timeout=120)
            if proceso.returncode:
                raise RuntimeError('pdftotext: ' + proceso.stderr.strip()[:250])
            sha = sha256_archivo(pdf)
            paginas = [x for x in proceso.stdout.split('\f') if x.strip()]
            registro['sha256_pdf'] = sha
            registro['paginas'] = len(paginas)
            extraidos = []
            for n, p in enumerate(paginas, 1):
                extraidos.extend(extraer_narracion(p, n, pdf.name, sha))
                extraidos.extend(extraer_tabla(p, n, pdf.name, sha))
            por_clave = {}
            for r in extraidos:
                clave = (r['periodo'], r['ambito'], r['porcentaje'])
                por_clave.setdefault(clave, r)
            extraidos = list(por_clave.values())
            hallazgos.extend(extraidos)
            registro['indicadores_detectados'] = len(extraidos)
            registro['estado'] = 'EXTRAIDO' if extraidos else 'SIN_EXTRACCION'
        except Exception as e:
            registro['estado'] = 'ERROR'
            registro['detalle'] = str(e)[:300]
        auditoria.append(registro)
        print('[{}/{}] {:<14} {:>3} registros | {}'.format(
              i, len(documentos), registro['estado'],
              registro['indicadores_detectados'], pdf.name), flush=True)

    # Conservar todas las fuentes. Consolidar solamente si hay coincidencia
    # de los valores publicados para un mismo período y ámbito.
    grupos = defaultdict(list)
    for r in hallazgos:
        grupos[(r['periodo'], r['ambito'])].append(r)
    consolidados, conflictos = [], []
    for (periodo, ambito), vals in sorted(grupos.items()):
        distintos = {r['porcentaje'] for r in vals}
        if len(distintos) > 1:
            conflictos.append(dict(periodo=periodo, ambito=ambito,
                                   valores=';'.join(str(v) for v in sorted(distintos)),
                                   documentos=';'.join(sorted({r['documento'] for r in vals}))))
            continue
        # Si existen fuentes concordantes, preservar conteo y referencias.
        elegido = sorted(vals, key=lambda r: (r['documento'], r['pagina_pdf']))[0].copy()
        elegido['fuentes_concordantes'] = len({r['documento'] for r in vals})
        consolidados.append(elegido)

    guardar_csv(salida/'indicadores_pdf.csv', hallazgos, CAMPOS)
    guardar_csv(salida/'auditoria_documentos.csv', auditoria,
                ['documento', 'sha256_pdf', 'paginas', 'indicadores_detectados',
                 'estado', 'detalle'])
    guardar_csv(salida/'conflictos.csv', conflictos,
                ['periodo', 'ambito', 'valores', 'documentos'])
    guardar_csv(salida/'consolidado_sin_conflictos.csv', consolidados,
                list(CAMPOS)+['fuentes_concordantes'])
    resumen = {'pdf_analizados': len(documentos),
               'pdf_con_extraccion': sum(r['estado']=='EXTRAIDO' for r in auditoria),
               'pdf_sin_extraccion': sum(r['estado']=='SIN_EXTRACCION' for r in auditoria),
               'pdf_con_error': sum(r['estado']=='ERROR' for r in auditoria),
               'registros_con_evidencia': len(hallazgos),
               'periodos_ambitos_consolidados': len(consolidados),
               'conflictos': len(conflictos),
               'por_metodo': dict(Counter(r['metodo'] for r in hallazgos)),
               'advertencia': 'Extraccion automatica preliminar: requiere validacion manual de muestra; NO es Silver publicado'}
    (salida/'resumen.json').write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\n=== RESUMEN ===')
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    print('Reportes:', salida)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--base', type=Path, default=ORIGEN)
    p.add_argument('--output', type=Path, default=DESTINO)
    a = p.parse_args()
    ejecutar(a.base, a.output)
