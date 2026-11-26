# OSIPTEL · Ingesta documental controlada v1

## Objetivo y límites

Este complemento mantiene el panel OSIPTEL en 127.0.0.1:8767 y agrega una ruta explícita para PDF **nuevos y pertinentes**:

**Subir y evaluar → publicar PDF original en Bronze/Raw → publicar evidencias candidatas en Silver → revisar evidencia por una persona → publicar un registro en Gold documental separado.**

No declara aprobaciones institucionales, no modifica las tablas Gold históricas F1 ni las seis tablas curadas F2/F3/F4. Tampoco incorpora automáticamente datos nuevos al dashboard de indicadores nacionales: ese paso requeriría validar semántica, cobertura, temporalidad y compatibilidad de unidades. Un PDF fuera de alcance o un PDF ya auditado es rechazado para evitar contaminación y duplicados.

## Instalación en la VM Oracle

1. Copiar `OSIPTEL_INGESTA_CONTROLADA_V1.zip` a `/srv/osiptel/scripts/` desde WSL.
2. Entrar a Oracle con `entrar-oracle`.
3. Ejecutar:

```bash
python3 -m zipfile -e /srv/osiptel/scripts/OSIPTEL_INGESTA_CONTROLADA_V1.zip /srv/osiptel/scripts/
bash /srv/osiptel/scripts/osiptel_ingesta_pdf_v1/instalar_en_oracle.sh
```

4. Reiniciar **únicamente** el panel viejo en 8767. Mantener el túnel SSH existente:

```bash
pkill -f '[p]ython3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py' || true
python3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py
```

Abrir http://127.0.0.1:8767 desde el equipo del usuario con túnel ya abierto.

## Prueba integral de un PDF nuevo

1. En «Subir y evaluar PDF», cargar una **resolución nueva** (por ejemplo, Resolución N.º 000069-2026-CD/OSIPTEL) y esperar a que aparezca una evidencia candidata.
2. En «Ingreso controlado», seleccionar **A · Publicar Bronze + Silver candidatos** y hacer clic en «Actualizar estado de ingesta». Esperar `BRONZE_SILVER_PUBLICADOS`.
3. Revisar manualmente el artículo, página y sentido de la resolución original.
4. Introducir nombre o identificador del revisor y una justificación de 25 caracteres o más; elegir el candidato correcto. Para F2 no se exige valor numérico. Para F3/F4 se requieren periodo, ámbito, unidad y valor, que deben aparecer en la página indicada. Para F4 la unidad debe ser `porcentaje_hogares` y el periodo `AAAA-Tn`.
5. Pulsar **B · Registrar revisión y publicar Gold documental**; se solicita confirmación explícita. Esperar `GOLD_DOCUMENTAL_CON_REVISION_REGISTRADA` y verificar `gold_estado`.
6. Repetir el ingreso del mismo PDF. Debe retornar `EXISTENTE_VERIFICADO` sin duplicar datos ni modificar el histórico.

Los endpoints son internos y están protegidos por token CSRF del panel:

- `GET /api/nuevo/estado?file=<id>`
- `POST /api/nuevo/ingestar?file=<id>`
- `POST /api/nuevo/aprobar?file=<id>` con justificación y revisor.

## Rutas HDFS (nuevas, separadas)

- Original inmutable: `/data/raw/pdf_controlados_v1/<sha256>/documento.pdf`
- Silver de evidencia candidata: `/data/silver/pdf_nuevos_candidatos_v1/<sha256>/v1`
- Gold de revisión registrada: `/data/gold/pdf_documental_revisado_v1/<sha256>/v1_<firma>`

La publicación utiliza Parquet Snappy en Silver/Gold, snapshots con `_SUCCESS`, verificación SHA del PDF en HDFS, escritura temporal y renombrado atómico. No se emplea `overwrite` ni `chmod 777`.

El estado local queda en `/srv/osiptel/evidence/osiptel_cierre_curado/pdf_nuevos_controlados/`.

## Qué NO garantiza

- Un documento fuera de la auditoría previa solo recibe clasificación y candidatos heurísticos hasta que una persona coteje la evidencia.
- Una revisión humana registrada no equivale a verificación de vigencia jurídica ni certificación formal de OSIPTEL o INEI.
- Una cifra documentada no entra automáticamente a los KPI nacionales F1/F4. Gold documental es un catálogo controlado **aparte**.
- PDF sin texto extraíble necesitan OCR o lectura humana; el programa rechaza extracciones parciales para este flujo.
- El panel es local y con CSRF, pero **no es un servicio multiusuario autenticado**. Mantener solo con túnel SSH, sin exponer 8767 a internet.

## Conservación de evidencias

No eliminar los PDF de cuarentena mientras se utilicen para revisión. El sistema conserva SHA-256, estado, texto candidato, revisión, justificación, copia de PDF original en HDFS y logs. El script utiliza `/tmp` dentro del contenedor para transferir archivos; las copias no son el origen canónico.
