import os
import json
import logging
import xml.etree.ElementTree as ET
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class RawProcessor:
    """Procesador para la capa RAW promocionando datos de Landing a Parquet de forma incremental."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _clean_spark_path(self, path: str) -> str:
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
        if not path:
            return ""
        clean = os.path.normpath(path).replace("\\", "/")
        if not clean.startswith("/lakehouse/default/"):
            if clean.startswith("Files/"):
                return f"/lakehouse/default/{clean}"
            return f"/lakehouse/default/Files/{clean.lstrip('/')}"
        return clean

    def landing_to_raw_json(self, landing_path: str, raw_path: Optional[str] = None, is_batch: bool = False) -> None:
        spark_raw = self._clean_spark_path(raw_path or ("Files/raw/batch/osm_roads" if is_batch else "Files/raw/realtime/weather"))
        spark_landing = self._clean_spark_path(landing_path)

        logging.info(f"📄 Leyendo JSONs desde Landing (recursivo): {spark_landing}")
        try:
            df = self.spark.read \
                .option("recursiveFileLookup", "true") \
                .option("multiline", "true") \
                .json(spark_landing)

            if df.count() > 0:
                now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
                df = df.withColumn("landing_source_file", F.col("_metadata.file_name").cast("string")) \
                       .withColumn("ingestion_timestamp", F.lit(now_str).cast("string"))
                
                df.write.format("parquet").mode("overwrite" if is_batch else "append").save(spark_raw)
                logging.info(f"✅ JSON guardado correctamente en Parquet RAW: {spark_raw}")
            else:
                logging.warning(f"⚠️ Sin datos JSON en ruta: {spark_landing}")
        except Exception as e:
            logging.error(f"❌ Error procesando JSON Landing ({landing_path}): {e}")

    def landing_to_raw_csv(self, landing_path: str, raw_path: Optional[str] = None, is_batch: bool = False) -> None:
        spark_raw = self._clean_spark_path(raw_path or ("Files/raw/batch/nasa_historical" if is_batch else "Files/raw/realtime/nasa_nrt"))
        spark_landing = self._clean_spark_path(landing_path)
        
        logging.info(f"📄 Leyendo CSV Landing: {spark_landing}")
        try:
            df = self.spark.read \
                .option("recursiveFileLookup", "true") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(spark_landing)
                
            if df.count() > 0:
                now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
                df = df.withColumn("landing_source_file", F.col("_metadata.file_name").cast("string")) \
                       .withColumn("ingestion_timestamp", F.lit(now_str).cast("string"))
                df.write.format("parquet").mode("overwrite" if is_batch else "append").save(spark_raw)
                logging.info(f"✅ CSV guardado en Parquet RAW: {spark_raw}")
            else:
                logging.warning(f"⚠️ Sin datos CSV en: {spark_landing}")
        except Exception as e:
            logging.error(f"❌ Error procesando CSV Landing ({landing_path}): {e}")

    def landing_to_raw_dgt_xml(self, landing_path: str, raw_path: Optional[str] = None) -> None:
        spark_raw = self._clean_spark_path(raw_path or "Files/raw/realtime/dgt_traffic")
        local_base = self._clean_local_path(landing_path)
        
        xml_files = []
        if os.path.isfile(local_base):
            xml_files.append(local_base)
        elif os.path.exists(local_base):
            for root, _, files in os.walk(local_base):
                for f in files:
                    if f.endswith(".xml"):
                        xml_files.append(os.path.join(root, f))

        if not xml_files:
            logging.warning(f"⚠️ No se encontraron XMLs de DGT en: {local_base}")
            return

        records = []
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        for xml_file in xml_files:
            file_name = os.path.basename(xml_file)
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()

                for elem in root.iter():
                    tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag_name in ["situationRecord", "situationRecordExtension"]:
                        row = {
                            "record_id": elem.attrib.get("id", None),
                            "landing_source_file": file_name,
                            "ingestion_timestamp": now_str
                        }
                        for child in elem.iter():
                            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                            if child.text and child.text.strip():
                                if child_tag not in row:
                                    row[child_tag] = child.text.strip()
                        if len(row) > 3:
                            records.append(row)
            except Exception as e:
                logging.error(f"⚠️ Error parseando XML {xml_file}: {e}")

        if records:
            df = self.spark.createDataFrame(records)
            df.write.format("parquet").mode("overwrite").save(spark_raw)
            logging.info(f"✅ XML DGT ({len(records)} registros) guardado en Parquet RAW.")
        else:
            logging.warning("⚠️ XML DGT sin registros válidos.")

    def landing_to_raw_osm(self, landing_path: str, raw_path: Optional[str] = None) -> None:
        spark_raw = self._clean_spark_path(raw_path or "Files/raw/batch/osm_roads")
        local_landing = self._clean_local_path(landing_path)
        
        if os.path.isdir(local_landing):
            json_files = []
            for root, _, files in os.walk(local_landing):
                for f in files:
                    if f.endswith(".json"):
                        json_files.append(os.path.join(root, f))
            if not json_files:
                logging.warning(f"⚠️ No se encontraron JSONs de OSM en: {landing_path}")
                return
            json_file = max(json_files, key=os.path.getmtime)
        else:
            json_file = local_landing

        logging.info(f"📄 Procesando JSON OSM desde: {json_file}")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            elements = data.get("elements", [])
            if not elements:
                logging.warning("⚠️ El JSON de OSM no contiene la clave 'elements'.")
                return

            now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
            flattened_records = []
            file_name = os.path.basename(json_file)
            for elem in elements:
                record = {
                    "id": elem.get("id"),
                    "type": elem.get("type"),
                    "landing_source_file": file_name,
                    "ingestion_timestamp": now_str
                }
                tags = elem.get("tags", {})
                if isinstance(tags, dict):
                    for k, v in tags.items():
                        record[f"tag_{k}"] = str(v)
                
                if "nodes" in elem:
                    record["nodes_count"] = len(elem["nodes"])
                flattened_records.append(record)

            df = self.spark.createDataFrame(flattened_records)
            df.write.format("parquet").mode("overwrite").save(spark_raw)
            logging.info(f"✅ OSM Red Viaria ({len(flattened_records)} tramos) guardada en RAW Parquet ({spark_raw}).")
        except Exception as e:
            logging.error(f"❌ Error procesando OSM Landing a RAW: {e}")