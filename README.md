# Ingeniería de Datos para la Gestión de Incendios 

> **Trabajo Fin de Máster (TFM)** — Máster en Big Data & Data Engineering  
> **Universidad Complutense de Madrid (UCM) & NTIC Master**  
> **Autora:** Paula Sobrados Risco  
> **Tutores:** Jorge Centeno y Alberto González  


## Descripción del Proyecto

Este repositorio contiene la librería Python y el código fuente desarrollado para la plataforma de ingeniería de datos sobre **Microsoft Fabric**. El sistema ingesta, procesa, correlaciona y analiza datos heterogéneos en tiempo casi real (NRT) y por lotes (Batch) para dar soporte a la toma de decisiones en situaciones de emergencia por incendios forestales en España.

### Fuentes de Datos Integradas
* **NASA FIRMS (VIIRS):** Anomalías térmicas y detecciones satelitales.
* **Open-Meteo API:** Predicciones meteorológicas horarias (viento, temperatura, humedad, lluvia).
* **DGT (DATEX II):** Incidencias de tráfico, retenciones y cortes en formato XML.
* **OpenStreetMap (Overpass API):** Red de carreteras nacional y atributos técnicos del vial.


## Arquitectura de Datos (Medallion & Lakehouse)

El proyecto implementa una arquitectura **Lakehouse** sobre **OneLake** en Microsoft Fabric dividida en 5 capas Delta Lake:

+-----------------------------------------------------------------------+
|                            DATA SOURCES                               |
|        NASA FIRMS       Open-Meteo        DGT DATEX II       OSM      |
+-----------------------------------------------------------------------+
│
▼
┌──────────────────────────────────────────────────────────────────────┐
│  Landing Layer  │ Guardado de archivos originales (JSON, CSV, XML)  │
└─────────────────┴────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────────────────┐
│  RAW Layer      │ Conversión inicial a formato Parquet               │
└─────────────────┴────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────────────────┐
│  Bronze Layer   │ Delta Lake, metadatos e idempotencia via MERGE     │
└─────────────────┴────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────────────────┐
│  Silver Layer   │ Normalización, desanidamiento y Clustering de fuego│
└─────────────────┴────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────────────────┐
│  Gold Layer     │ Vistas analíticas SQL para Power BI y Decisiones   │
└──────────────────────────────────────────────────────────────────────┘


---

## Retos Técnicos Destacados

1. **Clustering Espacio-Temporal de Incendios:** 
   Para evitar el coste computacional de comparaciones masivas $O(N^2)$, se implementó una estrategia híbrida basada en un **Grid Espacio-Temporal** (celdas de latitud/longitud y ventanas de 24h) combinado con la fórmula de **Haversine** ($\le 5\text{ km}$) y algoritmos de **componentes conexas (BFS/DFS)** para asignar un `cluster_id` único a cada incendio.
2. **Resiliencia ante Rate Limiting:** 
   Gestión automática del error HTTP `429 Too Many Requests` en Open-Meteo mediante reintentos orientados por la cabecera `Retry-After`.
3. **Parsing de XML Complejo (DATEX II):** 
   Aplanado nativo con `xml.etree.ElementTree` evitando fallos de inferencia de esquemas.
4. **Gestión del Ciclo de Vida de Incidencias DGT:** 
   Estrategia de estado para estampar `end_timestamp` cuando una incidencia desaparece de las ingestas NRT.


## Estructura del Proyecto (VS Code Workspace)

```text
tfm_mobility/
├── notebooks/                # Notebooks para orquestación en Microsoft Fabric
│   ├── 01_landing_*.py
│   ├── 02_raw_*.py
│   ├── 03_bronze_*.py
│   ├── 04_silver.py
│   └── 05_gold_*.py
├── src/
│   └── tfm_mobility/         # Paquete ejecutable principal de Python
│       ├── ingesters/        # Clientes HTTP e ingestas (Batch y NRT)
│       ├── processors/       # Transformaciones RAW, Bronze y Silver
│       ├── transform/        # Algoritmos ad-hoc (Clustering, Haversine, Grafos)
│       └── utils/            # Funciones auxiliares y logging
├── tests/                    # Suite de pruebas automatizadas
│   ├── test_ingesters.py
│   ├── test_processors.py
│   └── ...
├── pyproject.toml            # Configuración para empaquetar la librería (.whl)
├── .gitignore
└── README.md


## Pruebas unitarias y calidad de software

El proyecto incluye una suite completa de **31 pruebas unitarias** ejecutadas con pytest que cubren el 100% de las funcionalidades críticas (clientes HTTP, parsing XML, clustering y transformaciones PySpark).

Para ejecutar los tests localmente en VS Code:

1. **Crear y activar un entorno virtual:**

python -m venv .venv
source .venv/bin/activate  # En Linux/Mac
# o bien: .venv\Scripts\activate  # En Windows

2. **Instalar el paquete en modo editable con dependencias de desarrollo:**

pip install -e .[dev]

3. **Ejecutar las pruebas**

pytest tests/ -v


## Generación de Artefactos y Despliegue en Fabric

Para compilar el paquete Python y desplegarlo en el entorno de Microsoft Fabric:
python -m build



