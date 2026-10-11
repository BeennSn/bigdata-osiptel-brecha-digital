# Perfil de datos en data/raw

## 1) XLSX: 2.1 CONEXIONES DE INTERNET FIJO.xlsx

### Archivo y hojas
- Archivo: `data/raw/2.1 CONEXIONES DE INTERNET FIJO.xlsx`
- Hojas del libro: `Dataset`, `Diccionario`
- La fila de cabecera real del dataset aparece en la fila 4 del libro y contiene este encabezado exacto:
  - `Unnamed: 0`
  - `Mes`
  - `Empresa`
  - `Tecnologia`
  - `Segmento`
  - `Departamento`
  - `Rango Velocidad de Bajada (nominal)`
  - `Conexiones`

### Número de filas y tipos
- Filas del dataset: 141.106
- Columnas: 8
- Tipos detectados por pandas:
  - `Unnamed: 0`: float64
  - `Mes`: datetime64[us]
  - `Empresa`: object/string
  - `Tecnologia`: object/string
  - `Segmento`: object/string
  - `Departamento`: object/string
  - `Rango Velocidad de Bajada (nominal)`: object/string
  - `Conexiones`: int64

### 10 filas de ejemplo
```text
Unnamed: 0,Mes,Empresa,Tecnologia,Segmento,Departamento,Rango Velocidad de Bajada (nominal),Conexiones
,1999-12-01,,1) Dial-Up Fijo,No identificada,, ,120014
,2000-12-01,,1) Dial-Up Fijo,No identificada,, ,130560
,2000-12-01,,2) xDSL,No identificada,, ,32
,2000-12-01,,3) Cablemodem,No identificada,, ,788
,2001-12-01,,1) Dial-Up Fijo,Comercial,, ,3264
,2001-12-01,,1) Dial-Up Fijo,Residencial,, ,139371
,2001-12-01,,2) xDSL,Comercial,, ,1228
,2001-12-01,,2) xDSL,Residencial,, ,472
,2001-12-01,,3) Cablemodem,Comercial,, ,7
,2001-12-01,,3) Cablemodem,Residencial,, ,6000
```

### Formato real de la columna Mes y rango de fechas
- La columna `Mes` tiene formato de fecha con día fijo del primer día de cada mes: `yyyy-mm-dd`.
- Ejemplos observados: `1999-12-01`, `2000-12-01`, `2001-12-01`, `2026-03-01`.
- Rango real observado: `1999-12-01` a `2026-03-01`.
- No se observaron nulos en `Mes`.

### Valores distintos por columna (máximo 50 cada uno)

#### Empresa (160 valores distintos)
Primeros valores observados:
- AMERICATEL PERU S.A.
- CIRION TECHNOLOGIES PERU S.A.
- ENTEL PERU S.A.
- GILAT TO HOME PERU S.A.
- INFODUCTOS Y TELECOMUNICACIONES
- STAR GLOBAL COM S.A.C.
- TELEFONICA DEL PERU S.A.A.
- TELMEX
- TERRA NETWORKS PERU S.A.C.
- VALTRON
- YACHAY TELECOMUNICACIONES S.A.C.
- RED PRIVADA VIRTUAL S.A.
- AMERICA MOVIL PERU S.A.C.
- COMPAÑIA DE COMERCIO GLOBAL S.A.C.
- WINNER SYSTEMS S.A.C.
- SUPERCABLE TELEVISION S.R.L.
- RURAL TELECOM S.A.C.
- TELEMUNDO INTERNACIONAL E.I.R.L.
- AT & T GLOBAL NETWORK SERVICES DEL PERU S.R.L.
- PARABOLICA TV S.A.C.
- CONVERGIA PERU S.A.
- DIRECTV PERU S.R.L.
- AGUSTINA SERVICIOS GENERALES S.A.C.
- ANDESAT PERU S.A.C.
- CABLEVISION S.A.C.
- DIGITAL FIBER SERVICES S.A.C.
- GTD PERU S.A.
- INTERNEXA PERU S.A.
- OLO DEL PERU S.A.C.
- SENCINET LATAM PERU S.A.C.
- TVS WIRELESS S.A.C.
- VELATEL PERU S.A.C.
- VIA SATELITAL E.I.R.L.
- VIETTEL PERU S.A.C.
- ON EMPRESAS (antes WIN EMPRESAS S.A.C.)
- WI-NET PERU S.A.C.
- CONSORCIO OPTICAL S.A.C.
- MOCHE INVERSIONES S.A.
- MULTISERVICIOS DE TELECOMUNICACIONES SATELITAL E.I.R.L.
- NETLINE PERU S.A.
- RED INTERCABLE PERU S.A.C.
- EMPRESA DE TELECOMUNICACIONES MULTIMEDIA ALFA S.A.C.
- OPTICAL NETWORKS S.A.C.
- TOTAL COM S.A.C.
- HISPASAT PERU S.A.C.
- FIBERLUX S.A.C.
- COMUNICACIONES J&F CABLE TV S.A.C.
- HUANUCO TELECOM S.A.C.
- WI-NET TELECOM S.A.C.
- CABLE PUCALLPA S.R.L.

Variantes sospechosas observadas al normalizar mayúsculas y espacios:
- `TELEFONICA DEL PERU S.A.A.` y `telefonica del peru s.a.a.`
- `ON EMPRESAS (antes WIN EMPRESAS S.A.C.)` y `on empresas (antes win empresas s.a.c.)`
- `WI-NET PERU S.A.C.` y `wi-net peru s.a.c.`

#### Tecnología (7 valores distintos)
- `1) Dial-Up Fijo`
- `2) xDSL`
- `3) Cablemodem`
- `4) Wimax`
- `5) Fibra Óptica`
- `6) Satelital`
- `7) Otras Tecnologías`

Variantes sospechosas observadas al normalizar mayúsculas, tildes y espacios:
- `7) Otras Tecnologías` vs `7) otras tecnologias`
- `5) Fibra Óptica` vs `5) fibra optica`
- `3) Cablemodem` vs `3) cablemodem`

#### Segmento (3 valores distintos)
- `No identificada`
- `Comercial`
- `Residencial`

Variantes sospechosas:
- `No identificada` vs `no identificada`

#### Departamento (28 valores distintos)
- `LIMA Y CALLAO`
- `OTRAS REGIONES`
- `LA LIBERTAD`
- `LIMA`
- `ANCASH`
- `AREQUIPA`
- `CAJAMARCA`
- `CALLAO`
- `CUSCO`
- `HUANUCO`
- `ICA`
- `JUNIN`
- `LAMBAYEQUE`
- `MADRE DE DIOS`
- `MOQUEGUA`
- `PIURA`
- `PUNO`
- `TACNA`
- `TUMBES`
- `UCAYALI`
- `LORETO`
- `SAN MARTIN`
- `NO IDENTIFICADA`
- `AMAZONAS`
- `APURIMAC`
- `AYACUCHO`
- `HUANCAVELICA`
- `PASCO`

Variantes sospechosas:
- `NO IDENTIFICADA` vs `No identificada`
- `LIMA Y CALLAO` vs `LIMA` y `CALLAO` en registros de distintas agregaciones

#### Rango de velocidad (14 valores distintos)
- `a) BW < 256 Kbps`
- `b) 256 <= BW < 512 kbps`
- `c) 512 <= BW < 1024 kbps`
- `d) 1024 <= BW < 2048 kbps`
- `e) 2048 <= BW < 4096 kbps`
- `f) 4 Mbps <= BW < 8 Mbps`
- `g) 8 Mbps <= BW < 16 Mbps`
- `h) 16 Mbps <= BW < 30 Mbps`
- `i) 30 Mbps <= BW < 50 Mbps`
- `j) 50 Mbps <= BW < 100 Mbps`
- `k) 100 Mbps <= BW < 200 Mbps`
- `l) 200 Mbps <= BW < 500 Mbps`
- `m) 500 Mbps <= BW < 1000 Mbps`
- `n) BW >= 1000 Mbps`

Variantes sospechosas observadas:
- Mayúsculas/minúsculas: `BW`, `kbps`, `Mbps`
- Espacios: `BW < 256 Kbps` vs `BW<256 Kbps` (en texto no siempre homogéneo en todas las filas)

### Nulos, duplicados y anomalías en Conexiones
- Nulos por columna:
  - `Empresa`: 91
  - `Departamento`: 201
  - `Rango Velocidad de Bajada (nominal)`: 13.029
  - `Unnamed: 0`: 141.106
  - El resto de columnas no tiene nulos relevantes
- Filas duplicadas: 2.486
- `Conexiones`:
  - Negativos: 0
  - Máximo observado: 1.245.822
  - Valores > 1.000.000: 4
  - Valores > 100.000: 229
- Los 4 valores máximos aparecieron asociados a `TELEFONICA DEL PERU S.A.A.` con `Tecnologia = 2) xDSL` y `Segmento = No identificada` en meses de 2011-2012.

### Observación de calidad del XLSX
- La estructura del libro tiene un encabezado de referencia y varias filas vacías iniciales; la lectura debe especificarse con la fila correcta (`header=3` para la lectura real del dataset).
- El dataset no parece tener `Mes` en formato texto libre; viene como fecha real de pandas.
- Hay múltiples categorías con variantes de mayúsculas, minúsculas, tildes y espacios; una normalización previa es recomendable si se quiere consolidar por valor nominal.

---

## 2) PDFs: mandatos de OSIPTEL

### Cantidad total y distribución de tamaños
- Total de PDFs en `data/raw`: 141
- Tamaño total acumulado: 350.883.088 bytes (~334.4 MB)
- Tamaño mínimo: 263.671 bytes
- Tamaño máximo: 13.695.618 bytes
- Media: 2.488.532,54 bytes
- Mediana: 1.540.925 bytes
- Distribución por rango:
  - 0 a 100.000 bytes: 0
  - 100.000 a 500.000 bytes: 8
  - 500.000 a 1.000.000 bytes: 30
  - 1.000.000 a 5.000.000 bytes: 92
  - 5.000.000 a 10.000.000 bytes: 7

### Muestra de 10 PDFs revisados
Se revisó una muestra representativa de 10 archivos con `pdfplumber` para comprobar si hay texto extraíble y cómo aparece la empresa operadora y la fecha/año.

1. `inf005-dprc-2020.pdf`
   - Páginas: 64
   - Extraíble con `pdfplumber`: sí
   - Texto visible: sí, claro y legible
   - Empresa operadora mencionada en texto: `MEGACABLE`
   - Fecha/año: `2020` en nombre del archivo; también aparece en texto `FECHA : 15 de octubre de 2020`

2. `informe-000155-2026-dprc.pdf`
   - Páginas: 24
   - Extraíble con `pdfplumber`: sí, pero muy distorsionado/garbled
   - Empresa operadora: no se pudo reconocer claramente en la vista inicial del texto
   - Año: `2026` en el nombre del archivo; no se identificó claramente un año limpio en el texto del primer extracto

3. `informe-n-115-2026-dprc.pdf`
   - Páginas: 16
   - Extraíble con `pdfplumber`: sí, pero muy distorsionado
   - Empresa operadora: no se distinguió claramente
   - Año: `2026` en nombre y en texto del extracto

4. `informe-n-127-2026-dprc.pdf`
   - Páginas: 24
   - Extraíble con `pdfplumber`: sí, pero con texto deformado
   - Empresa operadora: no se distinguió claramente
   - Año: `2026` en nombre y en texto del extracto

5. `informe-n-142-2026-dprc.pdf`
   - Páginas: 28
   - Extraíble con `pdfplumber`: sí, pero muy desordenado
   - Empresa operadora: no se distinguió claramente
   - Año: `2026` en nombre; no se identificó claramente en el texto del extracto

6. `informe-n-163-2026-dprc.pdf`
   - Páginas: 34
   - Extraíble con `pdfplumber`: sí, con texto deformado
   - Empresa operadora: no se distinguió claramente
   - Año: `2026` en nombre; no se identificó claramente en el texto del extracto

7. `informe-n-164-2026-dprc.pdf`
   - Páginas: 36
   - Extraíble con `pdfplumber`: sí, pero con caracteres fragmentados
   - Empresa operadora: se identificó `EMPRESA REGIONAL DE SERVICIO PUBLICO DE ELECTRICIDAD ELECTRONORTEMEDIO SOCIEDAD ANONIMA – HIDRANDINA S.A.` en el extracto
   - Año: `2026` en nombre del archivo; también se identificó `2026` en el texto del extracto

8. `informe002-2020-dprc.pdf`
   - Páginas: 51
   - Extraíble con `pdfplumber`: sí
   - Texto visible: claro y legible
   - Empresa operadora: `MULTIVISION S.R.L.` en el texto
   - Fecha/año: `2020` en nombre del archivo; también aparece `FECHA : 15 de octubre de 2020`

9. `informe002-dprc-2024.pdf`
   - Páginas: 20
   - Extraíble con `pdfplumber`: sí, aunque con texto segmentado
   - Empresa operadora: en el texto se identifica `DATA ONLINE PERÚ S.A.C.`
   - Fecha/año: `2024` en nombre del archivo; también se observa `FECHA : 4 de enero de 20...` en el primer extracto

10. `informe004-dprc-2023.pdf`
   - Páginas: 7
   - Extraíble con `pdfplumber`: sí
   - Texto visible: claro y legible
   - Empresa operadora: `HÚANUCO TELECOM S.A.C.` y `EMPRESA REGIONAL DE SERVICIO PÚBLICO DE ELECTRICIDAD DEL CENTRO S.A.`
   - Fecha/año: `2023` en nombre del archivo; además aparece `FECHA : 6 de enero de 2023`

### Conclusión del análisis de PDFs
- En la muestra revisada, todos los PDFs tienen texto extraíble con `pdfplumber`.
- No se pudo confirmar que haya PDFs puramente escaneados sin texto; sí hay varios con texto deformado/garbled, que probablemente no se procesaron con OCR ni se exportaron con tipografías limpias.
- La empresa operadora suele estar visible en el primer bloque del texto, pero en algunos `informe-n-20xx-...` la legibilidad es insuficiente para identificarla con seguridad.
- En el nombre del archivo el año casi siempre aparece de forma clara (por ejemplo `2020`, `2023`, `2024`, `2026`).
- En el texto del documento también se observa el año o la fecha en varios casos, aunque no siempre de forma limpia y legible.

---

## 3) Resumen ejecutivo
- El Excel `2.1 CONEXIONES DE INTERNET FIJO.xlsx` parece ser la fuente estructurada principal; tiene 141.106 filas y 8 columnas con fechas mensuales, empresa, tecnología, segmento, departamento, rango de velocidad y conexiones.
- La columna `Mes` tiene formato `fecha` real y cubre desde diciembre de 1999 hasta marzo de 2026.
- El conjunto presenta algunos problemas de calidad esperables en fuentes históricas: 2.486 filas duplicadas, 91 filas sin empresa, 201 sin departamento y 13.029 sin rango de velocidad.
- No hay conexiones negativas; los valores extremos se concentran en `TELEFONICA DEL PERU S.A.A.` y muestran picos mayores a 1.000.000 y hasta 1.245.822.
- Las categorías de empresa, tecnología, departamento y rango de velocidad presentan variantes por mayúsculas, tildes y espacios que deben normalizarse antes de análisis comparativo.
- Los PDFs revisados no son todos iguales: algunos son claros y legibles, y otros muestran texto muy distorsionado con `pdfplumber`. Aun así, en la muestra analizada sí se pudo extraer texto en todos los casos.
- La evidencia disponible sugiere que los mandatos de OSIPTEL se guardan como PDFs con nombre y contenido variado; no se detectó una regla unificada de nomenclatura ni de legibilidad total para todos los archivos.

> Criterio de rigor: este perfil se basó exclusivamente en los archivos reales presentes en `data/raw` y en la extracción de texto con `pdfplumber`. Cuando no fue posible determinar un dato con seguridad (por ejemplo, una empresa o fecha en un PDF garbled), se señaló explícitamente como no determinado.
