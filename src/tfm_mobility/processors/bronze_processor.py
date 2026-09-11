import os
import re
import logging
from typing import List
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

#configuracion de logs para auditar las promociones de datos hacia las tablas delta
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class BronzeProcessor:
    """Procesador para la capa Bronze con sanitización de esquemas y registro forzado en el catálogo Delta."""

    def __init__(self, spark: SparkSession):
        #inicio la sesion de spark y activo el esquemamerge para soportar cambios de esquema automaticos
        self.spark = spark
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _resolve_fabric_path(self, path: str) -> str:
        #limpio la ruta introducida para ajustarla al formato de rutas relativas de fabric
        if not path:
            return ""
        clean = os.path.normpath(path).replace("\\", "/")
        if "Files/" in clean:
            clean = "Files/" + clean.split("Files/")[1]
        elif not clean.startswith("Files/"):
            clean = f"Files/{clean.lstrip('/')}"
        return clean

    def _sanitize_column_names(self, df: DataFrame) -> DataFrame:
        #he implementado esta limpieza de caracteres especiales en nombres de columna para evitar fallos en parquet y delta
        cleaned_cols = []
        for col_name in df.columns:
            clean_col = re.sub(r'[{}:;()\s\t=/\\]', '_', col_name)
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            cleaned_cols.append(clean_col)
        
        for old_col, new_col in zip(df.columns, cleaned_cols):
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def _read_raw_data(self, clean_path: str) -> DataFrame:
        """Lee datos Parquet estandarizados desde la capa RAW"""
        #se lee de forma recursiva los ficheros parquet que han sido generados en la capa raw
        try:
            df = self.spark.read.option("recursiveFileLookup", "true").parquet(clean_path)
            if df.take(1):
                logging.info(f"Leídos datos Parquet RAW desde: {clean_path}")
                return df
        except Exception as e:
            logging.warning(f"No se pudo leer Parquet en {clean_path}: {e}")
            
        return None

    def merge_into_bronze(self, raw_df: DataFrame, table_name: str, primary_keys: List[str]) -> None:
        #validacion basica para no ejecutar la operacion si el dataframe origen no trae registros
        if raw_df is None or not raw_df.take(1):
            logging.warning(f"El DataFrame para '{table_name}' está vacío. Se omite la operación.")
            return

        #limpieza de columnas y preparacion de las claves primarias para el cruce
        raw_df = self._sanitize_column_names(raw_df)
        
        sanitized_pks = [re.sub(r'_+', '_', re.sub(r'[{}:;()\s\t=/\\]', '_', pk)).strip('_') for pk in primary_keys]
        existing_pks = [pk for pk in sanitized_pks if pk in raw_df.columns]

        if not existing_pks:
            existing_pks = [raw_df.columns[0]]

        #deduplico en memoria quedandome con la ultima marca de ingesta antes de fusionar
        if "ingestion_timestamp" in raw_df.columns:
            dedup_raw_df = raw_df.orderBy(raw_df["ingestion_timestamp"].desc()).dropDuplicates(subset=existing_pks)
        else:
            dedup_raw_df = raw_df.dropDuplicates(subset=existing_pks)

        #si la tabla delta ya existe en el catalogo ejecuto la logica de merge condicional
        if self.spark.catalog.tableExists(table_name):
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in existing_pks])

            meta_cols = {"landing_source_file", "ingestion_timestamp", "updated_source_file", "updated_timestamp"}
            data_cols = [c for c in dedup_raw_df.columns if c not in existing_pks and c not in meta_cols]

            update_condition = " OR ".join([f"NOT (target.{c} <=> source.{c})" for c in data_cols]) if data_cols else "1 = 0"

            insert_values = {col: f"source.{col}" for col in dedup_raw_df.columns if col not in ["updated_source_file", "updated_timestamp"]}
            insert_values["updated_source_file"] = "CAST(NULL AS STRING)"
            insert_values["updated_timestamp"] = "CAST(NULL AS TIMESTAMP)"

            #construccion del mapa de actualizacion para estampar los metadatos de trazabilidad
            update_values = {col: f"source.{col}" for col in dedup_raw_df.columns if col not in ["landing_source_file", "ingestion_timestamp"]}
            update_values["landing_source_file"] = "target.landing_source_file"
            update_values["ingestion_timestamp"] = "target.ingestion_timestamp"
            update_values["updated_source_file"] = "source.landing_source_file"
            update_values["updated_timestamp"] = "current_timestamp()"

            #ejecucion de la sentencia merge de delta lake
            delta_table.alias("target") \
                .merge(dedup_raw_df.alias("source"), merge_condition) \
                .whenMatchedUpdate(condition=update_condition, set=update_values) \
                .whenNotMatchedInsert(values=insert_values) \
                .execute()
            logging.info(f"MERGE condicional completado en '{table_name}'.")
        else:
            #si es la primera ejecucion he programado la creacion inicial de la tabla delta
            initial_df = dedup_raw_df.selectExpr(
                "*",
                "CAST(NULL AS STRING) AS updated_source_file",
                "CAST(NULL AS TIMESTAMP) AS updated_timestamp"
            )
            initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            logging.info(f"✨ Tabla Delta '{table_name}' creada e inscrita en el catálogo por primera vez.")

        self.spark.catalog.refreshTable(table_name)

    def process_bronze_weather(self, raw_path: str = "Files/raw/realtime/weather") -> None:
        #proceso especifico para la tabla meteorologica seleccionando los atributos del json anidado
        clean_path = self._resolve_fabric_path(raw_path)
        logging.info(f"Procesando 'bronze_weather' desde: {clean_path}")

        try:
            df_raw = self._read_raw_data(clean_path)
            if df_raw is None or not df_raw.take(1):
                return

            df_bronze = df_raw.select(
                F.col("latitude"),
                F.col("longitude"),
                F.col("elevation"),
                F.col("timezone"),
                F.col("hourly"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            )

            #si detecto que la tabla existente no tiene el esquema correcto la borro para recrearla de forma limpia
            if self.spark.catalog.tableExists("bronze_weather"):
                existing_cols = self.spark.table("bronze_weather").columns
                if "hourly" not in existing_cols:
                    self.spark.sql("DROP TABLE IF EXISTS bronze_weather")

            self.merge_into_bronze(df_bronze, "bronze_weather", ["latitude", "longitude"])
        except Exception as e:
            logging.error(f"Error al procesar 'bronze_weather': {e}")

    def promote_realtime_to_bronze(self) -> None:
        #metodo de orquestacion para procesar todas las fuentes dinamicas en tiempo real
        self.process_bronze_weather("Files/raw/realtime/weather")

        sources = [
            ("Files/raw/realtime/dgt_traffic", "bronze_dgt_traffic", ["record_id"]),
            ("Files/raw/realtime/nasa_nrt", "bronze_nasa_nrt", ["latitude", "longitude", "acq_date", "acq_time"])
        ]

        for raw_path, table_name, pk in sources:
            clean_path = self._resolve_fabric_path(raw_path)
            try:
                raw_df = self._read_raw_data(clean_path)
                self.merge_into_bronze(raw_df, table_name, pk)
            except Exception as e:
                logging.error(f"Error procesando {table_name}: {e}")

    def promote_batch_to_bronze(self) -> None:
        #aqui promociono las fuentes batch hacia la capa bronze
        sources = [
            ("Files/raw/batch/nasa_historical", "bronze_nasa_historical", ["latitude", "longitude", "acq_date", "acq_time"]),
            ("Files/raw/batch/osm_roads", "bronze_osm_roads", ["id"]),
            ("Files/raw/batch/osm_places", "bronze_osm_places", ["id"])
        ]

        for raw_path, table_name, pk in sources:
            clean_path = self._resolve_fabric_path(raw_path)
            try:
                raw_df = self._read_raw_data(clean_path)
                self.merge_into_bronze(raw_df, table_name, pk)
            except Exception as e:
                logging.error(f"Error procesando {table_name}: {e}")