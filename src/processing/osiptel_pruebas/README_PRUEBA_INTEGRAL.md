# OSIPTEL — prueba integral y decisiones (sobre Oracle real)

Este complemento **no reemplaza** `osiptel_cierre` ya publicado ni modifica ningún CSV curado, Bronze, Silver, Gold, ni tablas publicadas. Se ejecuta junto a los archivos del commit `58dd8cd` y reutiliza HDFS y Docker/Spark existentes.

## 1. Qué certifica la prueba (6 etapas)

| Etapa | Prueba | Condición de aprobación |
|---|---|---|
| 01 | SHA-256 de todos los CSV y coherencia | `AUDITORIA_COHERENTE` |
| 02 | Seis snapshots HDFS y `_SUCCESS` | Seis rutas existen |
| 03 | Preflight Spark, sin publicar | `CONTROLES_OK` y código 0 |
| 04 | Ejecutar otra vez publicación | **Seis** `EXISTENTE_VERIFICADO`, misma versión y firmas |
| 05 | Consultar Gold F1 + seis tablas curadas y generar resultados | `DECISIONES_OK`, contadores y JSON reales |
| 06 | Interfaz original HTTP | HTML y APIs GET 200; CSRF inválido 403; archivo de prueba guardado en cuarentena y eliminado |

Se genera `/srv/osiptel/scripts/osiptel_pruebas/salida/reporte_pruebas.json` con estado global, etapas y duración medida. El análisis se guarda en `salida/decisiones.json` e incluye cuatro decisiones justificadas y un ranking **real** de departamentos (si hay datos suficientes).

**Nota:** antes de iniciar se aplica un ajuste mínimo y respaldado a `osiptel_cierre/ejecutar_cierre.py`: el modo preflight deja intacto `ultimo_resumen.json` publicado, en vez de sustituirlo por un reporte que no contiene publicaciones. Se guarda `.py.bak_antes_pruebas`.

## 2. Instalación en Oracle

En **WSL** de Windows, colocar el ZIP en Descargas:

```bash
scp -i ~/ssh-key-2026-03-26.key \
  /mnt/c/Users/Angel/Downloads/OSIPTEL_PRUEBA_INTEGRAL_DECISIONES.zip \
  ubuntu@158.101.36.142:/srv/osiptel/scripts/
entrar-oracle
```

**Dentro de Oracle, `ubuntu@angeldev:~$`:**

```bash
python3 -m zipfile -e \
  /srv/osiptel/scripts/OSIPTEL_PRUEBA_INTEGRAL_DECISIONES.zip \
  /srv/osiptel/scripts/
python3 -m py_compile /srv/osiptel/scripts/osiptel_pruebas/*.py
```

## 3. Abrir panel de pruebas y decisiones

En Oracle (sin cerrar la sesión):

```bash
python3 /srv/osiptel/scripts/osiptel_pruebas/panel_pruebas.py
```

En **otra terminal WSL** de Windows, sin entrar primero a Oracle:

```bash
ssh -N -L 8767:127.0.0.1:8767 \
  -i ~/ssh-key-2026-03-26.key \
  ubuntu@158.101.36.142
```

Abrir en Windows: http://127.0.0.1:8767

Pulsar **«1 · Probar todo (6 controles)»**. El panel muestra estado, registro, informe de pruebas y, cuando termine correctamente, las decisiones calculadas en Spark desde HDFS. **No** lanzar varias pruebas simultáneas. No abrir 8767 en el firewall de Oracle; el servicio escucha solo 127.0.0.1.

### Alternativa sin interfaz (misma prueba)

En una segunda terminal **Oracle**:

```bash
bash /srv/osiptel/scripts/osiptel_pruebas/ejecutar_pruebas.sh
```

Resultado esperado si todas pasan:

```text
PRUEBA_OK 01 CSV y SHA-256
PRUEBA_OK 02 HDFS _SUCCESS
PRUEBA_OK 03 Spark preflight
PRUEBA_OK 04 Publicacion idempotente
PRUEBA_OK 05 Indicadores y decisiones reales
PRUEBA_OK 06 Interfaz HTTP/CSRF/cuarentena
"estado": "APROBADO_TECNICAMENTE"
CODIGO_FINAL_PRUEBAS=0
```

Si la prueba falla, **no** repetir en bucle: enviar `salida/reporte_pruebas.json` y las últimas líneas de `salida/accion_ultima.log` o `salida/ultima_prueba_terminal.log`.

## 4. Interpretación correcta de las decisiones

- **D01, acceso rural:** compara 2026-T2 de INEI entre rural y Lima Metropolitana, en puntos porcentuales. Es una propuesta de **estudio diagnóstico**, no de desplegar infraestructura en un departamento concreto.
- **D02, conexiones fijas:** calcula los dos últimos cierres trimestrales Gold F1 y muestra departamentos con caída porcentual y base previa ≥1.000 conexiones. Una caída administrativa del stock no equivale a pérdida geográfica de cobertura.
- **D03, calidad:** mantiene 102 conflictos F4 y 74 series pendientes fuera de indicadores aprobados.
- **D04, regulación/operadores:** conserva 287 menciones F2 y 12 cifras F3 como contexto, nunca como proyectos concedidos ni sumas de magnitudes incompatibles.

La tabla nacional F1-F4 compara dos variables de diferente unidad: **conexiones fijas** vs **% de hogares con algún internet**. Nunca dividir, restar o inferir causalidad entre ambas. En los cinco trimestres verificados, **cuatro** tienen observación F1; para 2026-T2 solo hay INEI.

No hay modelo predictivo ML ni validación por expertos de la *decisión*; esas tareas son responsabilidad del análisis. El panel sí permite una decisión operativa inicial de **qué investigar primero** respaldada por evidencia.

## 5. Estado de la automatización

El conjunto F1/F2/F3/F4 existente puede prevalidarse y republicarse sin intervención en los archivos. **Los uploads nuevos se almacenan en cuarentena**; no está implementada su ingestión y aprobación completa Bronze→Silver→Gold. La prueba de upload demuestra precisamente este control, no una función que el código no ofrece.

## 6. Protección de datos y reversibilidad

- No sobrescribe `Gold`, `Silver`, `Bronze` ni mart curado. La segunda publicación debe resultar `EXISTENTE_VERIFICADO`.
- No ejecuta `git push`, borra snapshots ni altera CSV auditados.
- Retira solo el archivo de prueba artificial que ella misma crea en cuarentena.
- Los outputs JSON se guardan dentro de `osiptel_pruebas/salida/` del servidor OCI.
- Los cambios de la línea de preflight se respaldan una sola vez en `.py.bak_antes_pruebas`.
