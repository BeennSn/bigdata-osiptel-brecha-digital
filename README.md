# Pipeline Big Data: documentos regulatorios de OSIPTEL

Pipeline batch on-premise sobre PDFs públicos de OSIPTEL: ingesta, HDFS (Raw, Silver, Gold), Spark sobre YARN y análisis en JupyterLab.

## Equipo

| Integrante | Rol |
|---|---|
| Sinacay Nuñez, Benjamin | Coordinación |
| Casanova Chumpitaz, Angel Francisco | Análisis y modelado |
| León García, Axel Erico | Análisis y modelado |
| Carruitero Ortiz, Soraya Del Pilar | Calidad y documentación |
| Medina Sixce, Darli Manuel | Ingeniería de datos |

## Pipeline

| # | Etapa | Herramienta | Entrada | Salida |
|---|---|---|---|---|
| 1 | Descarga y Estructuración | Python (`run_pipeline.py`) | ZIP remoto (Google Drive) | `/data/raw/` (Staging local) |
| 2 | Ingesta a HDFS & Manifiesto | HDFS CLI / Python script | `/data/raw/` (Staging) | HDFS Raw (`/data/raw/osiptel_pdfs/`) + `data/manifest.json` |
| 3 | Procesamiento Raw a Silver | PySpark + YARN + `pdfplumber` | HDFS Raw (`/data/raw/osiptel_pdfs/`) | Capa Silver (`hdfs://localhost:9000/data/silver/osiptel_textos_limpios.parquet`) |
| 4 | Análisis Exploratorio & Reportes | JupyterLab + PySpark + Pandas | Capa Silver (Parquet) | Estadísticas, gráficos y consultas interactivas |
| 5 | Ingeniería de Características | Spark ML | Capa Silver | Capa Gold (Por definir / Siguientes fases) |
| 6 | Modelo Analítico | Spark ML | Capa Gold | Métricas y resultados del modelo |

---

## 🚀 Ejecución del Pipeline

Sigue estos pasos en tu terminal dentro de la máquina virtual para poner en marcha todo el flujo de trabajo, desde la instalación de dependencias hasta la generación de la Capa Silver en HDFS.

## 0. Descargar repositorio

```bash
git clone https://github.com/BeennSn/bigdata-osiptel-brecha-digital.git
cd bigdata-osiptel-brecha-digital
```

### 1. Preparación del entorno y dependencias


Asegúrate de instalar las librerías necesarias especificadas en el archivo de requerimientos dentro de tu entorno virtual:

```bash
# Activar el entorno virtual
source ~/venv-bd/bin/activate

# Instalar las dependencias del proyecto
pip install -r requirements.txt
```

### 2. Inicio de servicios de Big Data (Hadoop y YARN)

Levanta los servicios distribuidos necesarios para el almacenamiento en HDFS y la ejecución de trabajos con YARN:

```bash
start-dfs.sh
start-yarn.sh
```

> Puedes verificar que los procesos estén activos ejecutando `jps`.

### 3. Ejecución del Orquestador General

Una vez que el entorno y los servicios estén listos, ejecuta el script principal que automatiza todo el pipeline (descarga, estructuración, ingesta en HDFS, registro en el manifiesto y procesamiento distribuido con PySpark):

```bash
python run_pipeline.py
```

Al finalizar el proceso, los datos limpios y estructurados quedarán guardados y listos para su explotación en la Capa Silver:

```
hdfs://localhost:9000/data/silver/osiptel_textos_limpios.parquet
```



