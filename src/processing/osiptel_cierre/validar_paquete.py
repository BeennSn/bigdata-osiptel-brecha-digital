#!/usr/bin/env python3
"""Precontrol independiente, solo lectura, de datos revisados."""
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUTS = ROOT / 'inputs'
EXPECTED = {
    'F2_menciones_auditadas.csv': 287,
    'F3_12_cifras_corregidas_y_verificadas.csv': 12,
    'F4_20_indicadores_narrativa_verificados.csv': 20,
    'F4_94_series_con_estado_revision.csv': 94,
    'F4_102_conflictos_para_revision.csv': 102,
}

def require(condition, message):
    if not condition:
        raise RuntimeError(message)

def read(name):
    with (INPUTS / name).open('r', encoding='utf-8-sig', newline='') as fd:
        return list(csv.DictReader(fd))

def main():
    meta = json.loads((INPUTS / 'manifest_archivos.json').read_text(encoding='utf8'))
    for name, expected_count in EXPECTED.items():
        file = INPUTS / name
        require(file.is_file(), 'Falta '+name)
        sha = hashlib.sha256(file.read_bytes()).hexdigest()
        require(meta['files'][name]['sha256'] == sha, 'SHA no coincide: '+name)
        require(meta['files'][name]['bytes'] == file.stat().st_size, 'Bytes no coinciden: '+name)
        data = read(name)
        require(len(data) == expected_count, 'Conteo inesperado '+name)
        require(data and len(data[0]) > 3, 'Esquema incompleto '+name)
        print('CSV_OK',name,'filas='+str(len(data)),'SHA='+sha[:16],flush=True)
    f2=read('F2_menciones_auditadas.csv')
    for r in f2:
        require(r.get('verificacion_sha256')=='SI' and r.get('pagina_existe')=='SI' and r.get('termino_en_pagina') in ('SI','SI_PDFTOTEXT'), 'F2 sin evidencia corroborada: '+r.get('documento',''))
        require(r['apto_como_mandato_aprobado']=='NO', 'F2 aprobado indebidamente')
        require(re.fullmatch(r'[0-9a-f]{64}',r['sha256']) is not None, 'F2 hash PDF invalido')
    require(len({(r['documento'],r['sha256'],r['pagina_pdf'],r['linea_pdf'],r['tipo_hecho'],r['evidencia']) for r in f2})==287,'F2 hay duplicados en evidencias')
    f3=read('F3_12_cifras_corregidas_y_verificadas.csv')
    require(len({r['registro'] for r in f3})==12, 'F3 IDs duplicados')
    for r in f3:
        require(r['sha256_igual_original']=='SI' and r['valor_en_pagina']=='SI', 'F3 requiere revision')
        require(r['estado_revision']=='VALOR_Y_CONTEXTO_VERIFICADOS_EN_PDF','Estado F3 no verificado')
        require(r['indicador_corregido'] and r['periodo_validado'] and r['ambito_o_entidad'] and r['naturaleza'], 'Contexto F3 ausente')
        require(float(r['valor'])>=0, 'Valor F3 invalido')
        require(re.fullmatch(r'[0-9a-f]{64}',r['sha256_pdf']) is not None, 'F3 hash PDF invalido')
    f4=read('F4_20_indicadores_narrativa_verificados.csv')
    require(len({(r['periodo'],r['ambito']) for r in f4})==20,'F4 20 claves no unicas')
    periods=Counter(r['periodo'] for r in f4)
    require(periods==Counter({'2025-T1':4,'2025-T3':4,'2025-T4':4,'2026-T1':4,'2026-T2':4}), 'F4 trimestres no corresponden')
    for r in f4:
        require(r['estado_revision']=='VERIFICADO_EN_NARRATIVA_PDF' and r['evidencia_periodo_y_area']=='SI','F4 20 no verificado')
        require(0<=float(r['porcentaje'])<=100,'Porcentaje F4 invalido')
        require(re.fullmatch(r'[0-9a-f]{64}',r['sha256_pdf']) is not None, 'F4 hash PDF invalido')
    all94=read('F4_94_series_con_estado_revision.csv')
    agree={(r['periodo'],r['ambito'],r['porcentaje']) for r in all94}
    agree20={(r['periodo'],r['ambito'],r['porcentaje']) for r in f4}
    require(agree20.issubset(agree),'F4 20 no coincide con los 94')
    pending=[r for r in all94 if r['estado_revision']=='PENDIENTE_VALIDACION_SEMANTICA_DE_TABLA']
    require(len(pending)==74,'F4 pendientes !=74')
    require(len([r for r in all94 if r['estado_revision']=='VERIFICADO_EN_NARRATIVA_PDF'])==20,'F4 auditados !=20')
    conflicting=read('F4_102_conflictos_para_revision.csv')
    require(len({(r['periodo'],r['ambito']) for r in conflicting})==102,'F4 conflictos duplicados')
    require(not {(r['periodo'],r['ambito']) for r in conflicting}&{(r['periodo'],r['ambito']) for r in all94},'Conflictos solapan grupos sin conflicto')
    require(all(r['decision']=='EXCLUIDO_DE_GOLD_HASTA_RESOLUCION_DOCUMENTADA' for r in conflicting),'F4 conflicto marcado como aprobado')
    print('AUDITORIA_COHERENTE F2=287 F3=12 F4_VERIFICADOS=20 F4_PENDIENTES=74 F4_CONFLICTOS=102',flush=True)
    return 0

if __name__=='__main__':
    try:
        raise SystemExit(main())
    except Exception as ex:
        print('FALLO_VALIDACION:',ex,flush=True)
        raise SystemExit(1)
