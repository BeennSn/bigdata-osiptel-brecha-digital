from pathlib import Path
from collections import defaultdict, Counter
from decimal import Decimal, InvalidOperation
import csv, hashlib, json, re, sys

BASE = Path('/srv/osiptel/evidence/inei_fuente4_piloto')
PDFS = Path('/srv/osiptel/staging/extraido_da/da/Fuente 4')
DEST = Path('/srv/osiptel/evidence/inei_fuente4_depurado')
DEST.mkdir(parents=True, exist_ok=True)

def leer(nombre):
    with (BASE / nombre).open(newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def guardar(nombre, filas, columnas):
    with (DEST / nombre).open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=columnas, extrasaction='ignore')
        w.writeheader()
        w.writerows(filas)

def sha256(ruta):
    h = hashlib.sha256()
    with ruta.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

originales = leer('indicadores_pdf.csv')
auditoria = leer('auditoria_documentos.csv')
reporte_conflictos = leer('conflictos.csv')

# Verificar que los PDF originales corresponden con el inventario auditado.
hashes = defaultdict(set)
for p in PDFS.rglob('*'):
    if p.is_file() and p.suffix.lower() == '.pdf':
        hashes[p.name].add(sha256(p))

fallos = [r['documento'] for r in auditoria
          if r['sha256_pdf'] not in hashes.get(r['documento'], set())]
if fallos:
    print('ERROR: no coinciden o faltan PDF originales:', fallos[:10])
    sys.exit(2)

por_clave = defaultdict(list)
invalidos = []
for r in originales:
    motivo = ''
    try:
        valor = Decimal(r['porcentaje'].strip().replace(',', '.'))
        if not valor.is_finite() or not (0 <= valor <= 100):
            motivo = 'PORCENTAJE_FUERA_DE_RANGO'
        elif not re.fullmatch(r'\d{4}-T[1-4]', r['periodo'].strip()):
            motivo = 'PERIODO_INVALIDO'
        elif not r['ambito'].strip() or not r['indicador'].strip():
            motivo = 'DIMENSION_VACIA'
        elif r['unidad'] != 'porcentaje':
            motivo = 'UNIDAD_INESPERADA'
        elif not r['pagina_pdf'].isdigit() or int(r['pagina_pdf']) < 1:
            motivo = 'PAGINA_INVALIDA'
        elif r['sha256_pdf'] not in hashes.get(r['documento'], set()):
            motivo = 'PDF_SHA_NO_COINCIDE'
    except (InvalidOperation, KeyError, ValueError):
        motivo = 'FILA_MALFORMADA'
    if motivo:
        invalidos.append({**r, 'motivo_rechazo': motivo})
    else:
        clave = (r['periodo'], r['ambito'], r['indicador'])
        por_clave[clave].append((r, valor))

trazables, coherentes, conflictos = [], [], []
for (periodo, ambito, indicador), entradas in sorted(por_clave.items()):
    valores = sorted({valor for _, valor in entradas})
    hay_conflicto = len(valores) > 1
    estado = ('CONFLICTO_ENTRE_PUBLICACIONES' if hay_conflicto
              else 'SIN_CONFLICTO_DOCUMENTAL_POR_REVISAR')
    documentos = sorted({r['documento'] for r, _ in entradas})
    for r, valor in entradas:
        trazables.append({**r, 'estado_validacion': estado, 'apto_gold': 'NO'})
    if hay_conflicto:
        conflictos.append({
            'periodo': periodo, 'ambito': ambito, 'indicador': indicador,
            'valores': ';'.join(map(str, valores)),
            'documentos': ';'.join(documentos),
            'registros': len(entradas), 'estado_validacion': estado,
            'apto_gold': 'NO'
        })
    else:
        representante = min((r for r, _ in entradas),
                            key=lambda x: (x['documento'], int(x['pagina_pdf'])))
        coherentes.append({
            **representante, 'porcentaje': str(valores[0]),
            'documentos_concordantes': ';'.join(documentos),
            'numero_documentos': len(documentos),
            'numero_evidencias': len(entradas),
            'estado_validacion': estado, 'apto_gold': 'NO'
        })

columnas_originales = list(originales[0].keys())
guardar('indicadores_trazables.csv', trazables,
        columnas_originales + ['estado_validacion', 'apto_gold'])
guardar('indicadores_coherentes_preliminares.csv', coherentes,
        columnas_originales + ['documentos_concordantes', 'numero_documentos',
                              'numero_evidencias', 'estado_validacion', 'apto_gold'])
guardar('conflictos_para_revision.csv', conflictos,
        ['periodo', 'ambito', 'indicador', 'valores', 'documentos',
         'registros', 'estado_validacion', 'apto_gold'])
guardar('registros_invalidos.csv', invalidos,
        columnas_originales + ['motivo_rechazo'])

resumen = {
    'pdf_originales_verificados': len(auditoria),
    'registros_originales': len(originales),
    'registros_validos_en_formato': len(trazables),
    'registros_invalidos': len(invalidos),
    'series_sin_conflicto': len(coherentes),
    'grupos_con_conflicto': len(conflictos),
    'conflictos_reporte_anterior': len(reporte_conflictos),
    'coincide_con_reporte_anterior': len(conflictos) == len(reporte_conflictos),
    'registros_afectados_por_conflictos': sum(c['registros'] for c in conflictos),
    'estado': 'PRELIMINAR_CON_TRAZABILIDAD_NO_APTO_PARA_GOLD',
    'directorio': str(DEST)
}
(DEST / 'resumen.json').write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding='utf-8')
print(json.dumps(resumen, indent=2, ensure_ascii=False))
