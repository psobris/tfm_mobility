#Ingeniería de Datos para la Gestión de Incendios

Este repositorio contiene la plataforma de ingeniería de datos *end-to-end* desarrollada como Trabajo Fin de Máster para el **Máster en Big Data & Data Engineering de la Universidad Complutense de Madrid**.

La solución ingiere, procesa, limpia y relaciona datos de alertas térmicas de la **NASA FIRMS**, predicciones meteorológicas de **Open-Meteo**, incidencias de tráfico en tiempo real de la **DGT (DATEX II)** y la red viaria de **OpenStreetMap (OSM)** mediante una Arquitectura Medallion desplegada sobre **Microsoft Fabric**.

---

##Estructura del Repositorio

```text
tfm_mobility/
├── .github/              # Configuración y flujos de integración continua
├── notebooks/            # Notebooks de ejecucion en Microsoft Fabric
├── src/                  # Código fuente de la librería modular
│       ├── ingesters/    # Clientes de extracción para APIs (NASA, DGT, OSM, Weather)
│       ├── processors/   # Procesadores por capa (Raw, Bronze, Silver)
├── tests/                # Suite de 31 pruebas unitarias automatizadas con pytest
├── .gitignore            # Archivos excluidos del control de versiones
├── pyproject.toml        # Configuración de empaquetado del paquete Python (.whl)
└── README.md             # Documentación principal del proyecto