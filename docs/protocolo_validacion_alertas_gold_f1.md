# Protocolo de validación de alertas Gold OSIPTEL

## 1. Objetivo

Evaluar la utilidad de las alertas estadísticas de OSIPTEL Fuente 1 mediante una revisión documental independiente.

Se comparan dos estrategias:
- Método propuesto: umbrales de variación absoluta y porcentual.
- Referencia: selección de las 30 mayores variaciones absolutas.

## 2. Población y muestra

- Panel Gold: 611 combinaciones operador-departamento.
- Combinaciones elegibles: 156.
- Casos seleccionados por cada método: 30.
- Coincidencias entre métodos: 22.
- Unión de casos por revisar: 38.

La muestra está condicionada por ambas estrategias de selección. No es una muestra aleatoria de todas las combinaciones.

## 3. Procedimiento de revisión

1. Entregar al revisor la plantilla revision_ciega_gold_f1.csv.
2. No comunicar qué método seleccionó cada caso.
3. Identificar el operador, departamento y periodos evaluados.
4. Contrastar las cifras de diciembre de 2025 y marzo de 2026 con los registros originales.
5. Buscar evidencia documental independiente que permita explicar o confirmar la inconsistencia.
6. Registrar el dictamen, las fuentes consultadas y el tiempo efectivo de revisión.
7. Conservar evidencia verificable del dictamen.
8. Resolver discrepancias mediante una segunda revisión cuando sea posible.

## 4. Criterios de clasificación

CONFIRMADO:
Existe evidencia documental suficiente de una inconsistencia en las cifras, clasificación, periodo o procedimiento de reporte.

NO_CONFIRMADO:
La revisión encuentra que las cifras son consistentes con la evidencia disponible y no identifica una inconsistencia bajo el alcance aplicado.

INDETERMINADO:
La evidencia disponible es insuficiente o contradictoria para emitir un dictamen.

Una variación estadística elevada no constituye por sí misma una inconsistencia confirmada.

## 5. Indicadores

Una vez revisados los casos se calcularán:

- Cantidad de hallazgos confirmados por estrategia.
- Cantidad de casos indeterminados.
- Precisión@30, únicamente cuando los 30 casos de la estrategia tengan dictámenes concluyentes.
- Comparación de los casos exclusivos de cada estrategia.
- Tiempo efectivo observado de revisión.

Si existen dictámenes indeterminados se informará la cobertura de revisión y no se presentará una precisión definitiva.

No se calculará recall sobre las 156 combinaciones sin disponer de dictámenes del universo correspondiente.

## 6. Limitaciones

- Los umbrales empleados son reglas operativas propuestas.
- El ranking de referencia no representa necesariamente el procedimiento oficial de OSIPTEL.
- Las alertas no prueban fallas reales del servicio ni infracciones regulatorias.
- Los tiempos de procesamiento Spark no equivalen a tiempos de revisión humana.
- Agrupar alertas por operador es una propuesta de organización, no un ahorro de tiempo demostrado.
- La validación externa permanece pendiente hasta obtener dictámenes documentados.

## 7. Resultado esperado

Determinar si la regla propuesta identifica más inconsistencias documentales que la referencia, bajo el mismo presupuesto de 30 revisiones.

Los resultados deberán conservar los dictámenes y evidencias originales, sin modificar retrospectivamente las reglas evaluadas.
