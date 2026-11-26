#!/usr/bin/env python3
"""OSIPTEL PDF V2: clasificación temática y extracción jurídica con contexto.

No aprueba, no ingesta y no modifica HDFS. Mantiene la compatibilidad JSON
con el evaluador PDF V1 y evita falsos candidatos numéricos en contratos TIC.
"""
# OSIPTEL_PDF_CLASIFICACION_V2
from pathlib import Path
import re
import unicodedata
from datetime import datetime, timezone

import evaluar_pdf_v1 as legacy

# Evitar truncar documentos usuales de 49 páginas, sin extraer PDF enormes enteros.
legacy.MAX_PAGES = 80


def normalizar(s):
    s = unicodedata.normalize('NFKD', str(s))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return ' '.join(s.casefold().split())


def clasificar_analiticamente(pages):
    """Clasifica por contenido, no por el mero nombre OSIPTEL.

    Retorna categoria, familia, motivo; es una regla heuristica, NO un dictamen.
    """
    full = normalizar(' '.join(pages))
    comienzo = normalizar(' '.join(pages[:min(3, len(pages))]))

    licencias = (
        ('informe tecnico previo de evaluacion' in comienzo and 'software' in comienzo)
        or ('adobe creative cloud' in comienzo and ('licenciamiento' in comienzo or 'licencias' in comienzo))
    )
    # Un expediente de compra de software no mide brecha de internet fijo.
    if licencias:
        return ('FUERA_DE_ALCANCE_ANALITICO', 'FUERA_DE_ALCANCE',
                'Informe administrativo de evaluacion/licenciamiento de software; '
                'no mide conexiones fijas, cobertura ni acceso de hogares.')

    if 'hogares' in full and ('acceso a internet' in full or 'internet en los hogares' in full):
        return ('ESTADISTICA_HOGARES_CANDIDATA', 'F4',
                'Documento estadistico sobre acceso a internet de hogares; '
                'requiere comprobar definicion, ambito y periodo.')

    tema_fijo = bool(re.search(r'\b(internet fijo|conexiones fijas|acceso a internet fijo)\b', full))
    norma = ('se resuelve' in full or 'resolucion de consejo directivo' in full)
    if tema_fijo and norma:
        return ('NORMA_TECOM_CANDIDATA', 'F2',
                'Resolucion o documento regulatorio relacionado con internet fijo; '
                'distinguir proyecto, considerandos y articulos resolutivos.')

    infraestructura = bool(re.search(
        r'\b(mandato de comparticion|comparticion de infraestructura|mandato de acceso|'
        r'uso compartido de infraestructura)\b', full))
    if infraestructura:
        return ('ACTUACION_REGULATORIA_CANDIDATA', 'F2',
                'Documento que menciona comparticion de infraestructura o mandato; '
                'la mencion no equivale a mandato emitido.')

    if tema_fijo and ('conexiones' in full or 'fibra optica' in full):
        return ('ESTADISTICA_INTERNET_FIJO_CANDIDATA', 'F3',
                'Documento con datos de conexiones o fibra optica; '
                'requiere identificar periodos, unidades y operadores.')

    return ('REVISION_TEMATICA_NECESARIA', 'NO_DETERMINADA',
            'No se hallaron indicadores de internet fijo, estadisticas de hogares '
            'ni actos regulatorios claramente pertinentes en el texto extraido.')


def articulos_dispositivos(pages):
    """Busca el articulo resolutivo, NO considerandos ni encabezados casuales.

    Los resultados son candidatos de evidencia; no se asume legalidad ni vigencia.
    """
    out = []
    for num, page in enumerate(pages, 1):
        if 'se resuelve' not in normalizar(page):
            continue
        flat = ' '.join(page.split())
        normalized = normalizar(flat)
        # norm() conserva caracteres simples, pero puede alterar offsets si hay tildes;
        # para el fragmento capturamos con regex directamente sobre el texto original.
        rule = re.compile(r'\bart[ií]culo\s+(?:primero|1)\s*[.\-–º°:]*\s*'
                          r'(derogar|aprobar|modificar|disponer|dejar\s+sin\s+efecto)\b', re.I)
        match = rule.search(flat)
        if not match:
            continue
        fragmento = flat[match.start():match.start()+440]
        tema = normalizar(fragmento)
        if not any(term in tema for term in ('internet fijo','telefonica del peru','integratel',
                                             'telecomunicacion', 'comparticion')):
            continue
        verbo = normalizar(match.group(1)).upper().replace(' ', '_')
        out.append({
            'pagina': num,
            'tipo': 'DECISION_REGULATORIA_ARTICULO_RESOLUTIVO',
            'accion_detectada': verbo,
            'fragmento': fragmento,
            'estado': 'CANDIDATO_DOCUMENTAL_PENDIENTE_COTEJO',
            'nota': 'Detectado en articulo resolutivo; no certificado ni incorporado a Gold.',
        })
    return out


def evaluar_archivo(path):
    resultado = legacy.evaluar_archivo(path)
    # Un SHA verificado anteriormente no debe cambiar de estado con una regla heuristica.
    if resultado['registros_auditados_encontrados']:
        resultado['clasificacion_documental_v2'] = 'COINCIDENCIA_SHA_AUDITORIA_PREVIA'
        resultado['metodo_clasificacion'] = 'SHA256_VERIFICADO_CON_CATALOGO'
        return resultado

    total, pages = legacy.inspect_pdf(Path(path))
    category, family, reason = clasificar_analiticamente(pages)
    resultado['clasificacion_documental_v2'] = category
    resultado['metodo_clasificacion'] = 'REGLAS_HEURISTICAS_SOBRE_TEXTO_PDF'
    resultado['motivo_clasificacion'] = reason
    resultado['fuentes_clasificadas'] = [family]
    resultado['familias_candidatas'] = [family]
    resultado['paginas_totales'] = total
    resultado['paginas_extraidas'] = len(pages)
    resultado['hallazgos_auditoria'] = []
    resultado['registros_auditados_encontrados'] = 0
    resultado['registros_verificados_de_auditoria'] = 0
    resultado['conflictos_documentales_asociados'] = []
    resultado['aprobacion_institucional'] = False
    resultado['ingestado_hdfs'] = False

    if category == 'FUERA_DE_ALCANCE_ANALITICO':
        resultado['estado'] = 'FUERA_DE_ALCANCE_ANALITICO'
        resultado['candidatos_nuevos'] = []
        resultado['observaciones'] = [
            'PDF clasificado como ajeno a indicadores de brecha digital e internet fijo.',
            reason,
            'No se interpretan precios de licencias de software como metricas de telecomunicaciones.',
            'No se modifico HDFS, Bronze, Silver ni Gold; PDF permanece en cuarentena.',
        ]
        return resultado

    if category == 'NORMA_TECOM_CANDIDATA':
        hallazgos = articulos_dispositivos(pages)
        if hallazgos:
            resultado['estado'] = 'PDF_NUEVO_ACTO_REGULATORIO_DETECTADO_PENDIENTE_COTEJO'
            resultado['candidatos_nuevos'] = hallazgos[:legacy.MAX_HALLAZGOS]
            resultado['observaciones'] = [
                'Se localizo un articulo resolutivo; la accion debe cotejarse con el PDF original.',
                'No confundir derogacion de norma con mandato de comparticion ni con nueva cobertura.',
                'Documento no auditado por SHA-256; no existe aprobacion automatica.',
                'No se modifico HDFS, Bronze, Silver ni Gold; PDF permanece en cuarentena.',
            ]
            return resultado

    # Las otras familias mantienen candidatos preliminares del extractor anterior.
    resultado['estado'] = 'PDF_NUEVO_EXTRACCION_PRELIMINAR_SIN_APROBACION'
    resultado['observaciones'] = [
        reason,
        'No se ha validado la pertinencia/semantica de cada candidato.',
        'No se modifico HDFS, Bronze, Silver ni Gold; PDF permanece en cuarentena.',
    ]
    if not any(p.strip() for p in pages):
        resultado['observaciones'].append('PDF sin texto extraible: requiere OCR/revision visual.')
    if total > len(pages):
        resultado['observaciones'].append('Extraccion parcial: se inspeccionaron '
                                          f'{len(pages)} de {total} paginas.')
    return resultado


if __name__ == '__main__':
    import json, sys
    print(json.dumps(evaluar_archivo(Path(sys.argv[1])), indent=2, ensure_ascii=False))
