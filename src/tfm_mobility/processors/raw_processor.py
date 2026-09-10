import os
import json
import logging
import xml.etree.ElementTree as ET
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

#limpio el formato del logger para registrar todo el proceso de promocion a la capa raw
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class RawProcessor:
    """Procesador para la capa RAW promocionando datos de Landing a Parquet de forma incremental."""

    def __init__(self, spark: SparkSession):
        #inicializacion del procesador guardando la sesion activa de spark
        self.spark = spark

    def _clean_spark_path(self, path: str) -> str:
        #he construido este metodo para estandarizar las rutas que entiende spark en el entorno de fabric
        if not path:
            return ""
        clean = os.path.normpath(path).replace("\\", "/")
        for prefix in ["/lakehouse/default/", "lakehouse/default/", "/Files/"]:
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
        if not clean.startswith("Files/"):
            clean = f"Files/{clean.lstrip('/')}"
        return clean

    def _clean_local_path(self, path: str) -> str:
        #se ajusta la ruta fisica local necesaria para las operaciones del sistema de archivos con python puro
        if not path:
            return ""
        clean = os.path.normpath(path).replace("\\", "/")
        if not clean.startswith("/lakehouse/default/"):
            if clean.startswith("Files/"):
                return f"/lakehouse/default/{clean}"
            return f"/lakehouse/default/Files/{clean.lstrip('/')}"
        return clean

    def landing_to_raw_json(self, landing_path: str, raw_path: Optional[str] = None, is_batch: bool = False) -> None:
        #aqui limpio las rutas origen y destino para procesar archivos json procedentes de la capa landing
        spark_raw = self._clean_spark_path(raw_path or ("Files/raw/batch/osm_roads" if is_batch else "Files/raw/realtime/weather"))
        spark_landing = self._clean_spark_path(landing_path)

        logging.info(f"Leyendo JSONs desde Landing (recursivo): {spark_landing}")
        try:
            #lectura recursiva de los ficheros json utilizando la capacidad distribuida de spark
            df = self.spark.read \
                .option("recursiveFileLookup", "true") \
                .option("multiline", "true") \
                .json(spark_landing)

            if df.count() > 0:
                #he añadido los metadatos de auditoria del fichero de origen y la fecha exacta de ingesta
                now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
                df = df.withColumn("landing_source_file", F.col("_metadata.file_name").cast("string")) \
                       .withColumn("ingestion_timestamp", F.lit(now_str).cast("string"))
                
                #persistencia en formato parquet columnar aplicando la estrategia overwrite o append segun sea batch o nrt
                df.write.format("parquet").mode("overwrite" if is_batch else "append").save(spark_raw)
                logging.info(f"JSON guardado correctamente en Parquet RAW: {spark_raw}")
            else:
                logging.warning(f"Sin datos JSON en ruta: {spark_landing}")
        except Exception as e:
            logging.error(f"Error procesando JSON Landing ({landing_path}): {e}")

    def landing_to_raw_csv(self, landing_path: str, raw_path: Optional[str] = None, is_batch: bool = False) -> None:
        #preparacion de rutas para la promocion de ficheros csv de la nasa hacia la zona raw
        spark_raw = self._clean_spark_path(raw_path or ("Files/raw/batch/nasa_historical" if is_batch else "Files/raw/realtime/nasa_nrt"))
        spark_landing = self._clean_spark_path(landing_path)
        
        logging.info(f"Leyendo CSV Landing: {spark_landing}")
        try:
            #se realiza la lectura de los csv infiriendo el esquema e identificando cabeceras
            df = self.spark.read \
                .option("recursiveFileLookup", "true") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(spark_landing)
                
            if df.count() > 0:
                #estampado de campos de trazabilidad para auditar la procedencia del dato
                now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
                df = df.withColumn("landing_source_file", F.col("_metadata.file_name").cast("string")) \
                       .withColumn("ingestion_timestamp", F.lit(now_str).cast("string"))
                df.write.format("parquet").mode("overwrite" if is_batch else "append").save(spark_raw)
                logging.info(f"CSV guardado en Parquet RAW: {spark_raw}")
            else:
                logging.warning(f"Sin datos CSV en: {spark_landing}")
        except Exception as e:
            logging.error(f"Error procesando CSV Landing ({landing_path}): {e}")

    def landing_to_raw_dgt_xml(self, landing_path: str, raw_path: Optional[str] = None) -> None:
        #construccion de rutas locales para procesar la estructura xml compleja de la dgt
        spark_raw = self._clean_spark_path(raw_path or "Files/raw/realtime/dgt_traffic")
        local_base = self._clean_local_path(landing_path)
        
        #recorrido del directorio para recuperar todos los archivos xml almacenados en landing
        xml_files = []
        if os.path.isfile(local_base):
            xml_files.append(local_base)
        elif os.path.exists(local_base):
            for root, _, files in os.walk(local_base):
                for f in files:
                    if f.endswith(".xml"):
                        xml_files.append(os.path.join(root, f))

        if not xml_files:
            logging.warning(f"No se encontraron XMLs de DGT en: {local_base}")
            return

        records = []
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #he desarrollado este parser nativo con elementtree para desanidar el formato datex ii sin perder informacion
        for xml_file in xml_files:
            file_name = os.path.basename(xml_file)
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()

                #extraigo los nodos de incidencias eliminando los namespaces dinámicos de los tags
                for elem in root.iter():
                    tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag_name in ["situationRecord", "situationRecordExtension"]:
                        row = {
                            "record_id": elem.attrib.get("id", None),
                            "landing_source_file": file_name,
                            "ingestion_timestamp": now_str
                        }
                        #aplanamiento de las etiquetas hijas a un diccionario unificado
                        for child in elem.iter():
                            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                            if child.text and child.text.strip():
                                if child_tag not in row:
                                    row[child_tag] = child.text.strip()
                        if len(row) > 3:
                            records.append(row)
            except Exception as e:
                logging.error(f"Error parseando XML {xml_file}: {e}")

        #conversion de la lista de diccionarios planos a dataframe de spark y guardado en parquet
        if records:
            df = self.spark.createDataFrame(records)
            df.write.format("parquet").mode("overwrite").save(spark_raw)
            logging.info(f"DGT ({len(records)} registros) guardado en Parquet RAW")
        else:
            logging.warning("DGT sin registros validos")

    def landing_to_raw_osm(self, landing_path: str, raw_path: Optional[str] = None) -> None:
        #aqui resuelvo la ruta del fichero json de openstreetmap buscando el archivo mas reciente
        spark_raw = self._clean_spark_path(raw_path or "Files/raw/batch/osm_roads")
        local_landing = self._clean_local_path(landing_path)
        
        if os.path.isdir(local_landing):
            json_files = []
            for root, _, files in os.walk(local_landing):
                for f in files:
                    if f.endswith(".json"):
                        json_files.append(os.path.join(root, f))
            if not json_files:
                logging.warning(f"No se encontraron JSONs de OSM en: {landing_path}")
                return
            json_file = max(json_files, key=os.path.getmtime)
        else:
            json_file = local_landing

        logging.info(f"Procesando OSM desde: {json_file}")
        try:
            #lectura del json crudo recuperando el vector principal de elementos de la infraestructura viaria
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            elements = data.get("elements", [])
            if not elements:
                logging.warning("El JSON de OSM no contiene la clave 'elements'.")
                return

            now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
            file_name = os.path.basename(json_file)
            
            flattened_records = []
            
            #he filtrado y aplanado las etiquetas variables de osm hacia un subconjunto estricto de columnas
            for elem in elements:
                tags = elem.get("tags", {}) if isinstance(elem.get("tags"), dict) else {}
                
                #construccion del registro plano para evitar esquemas dispersos con nulos
                record = {
                    "id": int(elem.get("id")) if elem.get("id") is not None else None,
                    "type": str(elem.get("type")) if elem.get("type") else "way",
                    "nodes_count": len(elem.get("nodes", [])) if "nodes" in elem else 0,
                    "tag_highway": str(tags.get("highway")) if tags.get("highway") else None,
                    "tag_ref": str(tags.get("ref")) if tags.get("ref") else None,
                    "tag_name": str(tags.get("name")) if tags.get("name") else None,
                    "tag_maxspeed": str(tags.get("maxspeed")) if tags.get("maxspeed") else None,
                    "tag_lanes": str(tags.get("lanes")) if tags.get("lanes") else None,
                    "tag_oneway": str(tags.get("oneway")) if tags.get("oneway") else None,
                    "tag_surface": str(tags.get("surface")) if tags.get("surface") else None,
                    "tag_bridge": str(tags.get("bridge")) if tags.get("bridge") else None,
                    "tag_tunnel": str(tags.get("tunnel")) if tags.get("tunnel") else None,
                    "geometry_json": json.dumps(elem.get("geometry")) if "geometry" in elem else None,
                    "landing_source_file": file_name,
                    "ingestion_timestamp": now_str
                }
                
                flattened_records.append(record)

            #aqui defino un structtype explicito garantizando la estabilidad de la tabla parquet resultante
            osm_schema = StructType([
                StructField("id", LongType(), True),
                StructField("type", StringType(), True),
                StructField("nodes_count", IntegerType(), True),
                StructField("tag_highway", StringType(), True),
                StructField("tag_ref", StringType(), True),
                StructField("tag_name", StringType(), True),
                StructField("tag_maxspeed", StringType(), True),
                StructField("tag_lanes", StringType(), True),
                StructField("tag_oneway", StringType(), True),
                StructField("tag_surface", StringType(), True),
                StructField("tag_bridge", StringType(), True),
                StructField("tag_tunnel", StringType(), True),
                StructField("geometry_json", StringType(), True),
                StructField("landing_source_file", StringType(), True),
                StructField("ingestion_timestamp", StringType(), True)
            ])

            #creacion del dataframe con el esquema tipado y persistencia en la zona raw
            df = self.spark.createDataFrame(flattened_records, schema=osm_schema)
            
            df.write.format("parquet").mode("overwrite").save(spark_raw)
            logging.info(f"✅ OSM Red Viaria ({len(flattened_records)} tramos procesados) guardada en RAW Parquet ({spark_raw}).")
            
        except Exception as e:
            logging.error(f"Error procesando OSM Landing a RAW: {e}")
            raise