import os
import re
import logging
from typing import List
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class BronzeProcessor:
    """Procesador para la capa Bronze con sanitización de esquemas y Delta MERGE condicional."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _resolve_fabric_path(self, path: str) -> str:
        if not path:
            return ""
        clean = os.path.normpath(path).replace("\\", "/")
        if "Files/" in clean:
            clean = "Files/" + clean.split("Files/")[1]
        elif not clean.startswith("Files/"):
            clean = f"Files/{clean.lstrip('/')}"
        return clean

    def _sanitize_column_names(self, df: DataFrame) -> DataFrame:
        cleaned_cols = []
        for col_name in df.columns:
            clean_col = re.sub(r'[{}:;()\s\t=/\\]', '_', col_name)
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            cleaned_cols.append(clean_col)
        
        for old_col, new_col in zip(df.columns, cleaned_cols):
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def merge_into_bronze(self, raw_df: DataFrame, table_name: str, primary_keys: List[str]) -> None:
        if raw_df is None or raw_df.rdd.isEmpty():
            logging.warning(f"⚠️ El DataFrame para {table_name} está vacío. Cancelando MERGE.")
            return

        raw_df = self._sanitize_column_names(raw_df)
        sanitized_pks = [re.sub(r'_+', '_', re.sub(r'[{}:;()\s\t=/\\]', '_', pk)).strip('_') for pk in primary_keys]
        sanitized_pks = [pk for pk in sanitized_pks if pk in raw_df.columns]

        if not sanitized_pks:
            sanitized_pks = raw_df.columns[:2]

        if "ingestion_timestamp" in raw_df.columns:
            dedup_raw_df = raw_df.orderBy(raw_df["ingestion_timestamp"].desc()).dropDuplicates(subset=sanitized_pks)
        else:
            dedup_raw_df = raw_df.dropDuplicates(subset=sanitized_pks)

        if self.spark.catalog.tableExists(table_name):
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in sanitized_pks])

            meta_cols = {"landing_source_file", "ingestion_timestamp", "updated_source_file", "updated_timestamp"}
            data_cols = [c for c in dedup_raw_df.columns if c not in sanitized_pks and c not in meta_cols]

            update_condition = " OR ".join([f"NOT (target.{c} <=> source.{c})" for c in data_cols]) if data_cols else "1 = 0"

            insert_values = {col: f"source.{col}" for col in dedup_raw_df.columns if col not in ["updated_source_file", "updated_timestamp"]}
            insert_values["updated_source_file"] = "CAST(NULL AS STRING)"
            insert_values["updated_timestamp"] = "CAST(NULL AS TIMESTAMP)"

            update_values = {col: f"source.{col}" for col in dedup_raw_df.columns if col not in ["landing_source_file", "ingestion_timestamp"]}
            update_values["landing_source_file"] = "target.landing_source_file"
            update_values["ingestion_timestamp"] = "target.ingestion_timestamp"
            update_values["updated_source_file"] = "source.landing_source_file"
            update_values["updated_timestamp"] = "current_timestamp()"

            delta_table.alias("target") \
                .merge(dedup_raw_df.alias("source"), merge_condition) \
                .whenMatchedUpdate(condition=update_condition, set=update_values) \
                .whenNotMatchedInsert(values=insert_values) \
                .execute()
            logging.info(f"✅ MERGE condicional completado en '{table_name}'.")
        else:
            initial_df = dedup_raw_df.selectExpr(
                "*",
                "CAST(NULL AS STRING) AS updated_source_file",
                "CAST(NULL AS TIMESTAMP) AS updated_timestamp"
            )
            initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            logging.info(f"✨ Tabla Delta '{table_name}' creada por primera vez.")

    def process_bronze_weather(self, raw_path: str = "Files/raw/realtime/weather") -> None:
        clean_path = self._resolve_fabric_path(raw_path)
        logging.info(f"⚙️ Procesando 'bronze_weather' manteniendo estructura nativa desde: {clean_path}")

        try:
            df_raw = self.spark.read.option("recursiveFileLookup", "true").parquet(clean_path)
            
            # Mantener objeto 'hourly' nativo (fidelidad de origen)
            df_bronze = df_raw.select(
                F.col("latitude"),
                F.col("longitude"),
                F.col("elevation"),
                F.col("timezone"),
                F.col("hourly"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            )

            self.merge_into_bronze(df_bronze, "bronze_weather", ["latitude", "longitude"])
        except Exception as e:
            logging.error(f"❌ Error al procesar 'bronze_weather': {e}")

    def promote_realtime_to_bronze(self) -> None:
        self.process_bronze_weather("Files/raw/realtime/weather")

        sources = [
            ("Files/raw/realtime/dgt_traffic", "bronze_dgt_traffic", ["record_id"]),
            ("Files/raw/realtime/nasa_nrt", "bronze_nasa_nrt", ["latitude", "longitude", "acq_date", "acq_time"])
        ]

        for raw_path, table_name, pk in sources:
            clean_path = self._resolve_fabric_path(raw_path)
            try:
                raw_df = self.spark.read.option("recursiveFileLookup", "true").parquet(clean_path)
                self.merge_into_bronze(raw_df, table_name, pk)
            except Exception as e:
                logging.error(f"❌ Error procesando {table_name}: {e}")

    def promote_batch_to_bronze(self) -> None:
        sources = [
            ("Files/raw/batch/nasa_historical", "bronze_nasa_historical", ["latitude", "longitude", "acq_date", "acq_time"]),
            ("Files/raw/batch/osm_roads", "bronze_osm_roads", ["id"])
        ]

        for raw_path, table_name, pk in sources:
            clean_path = self._resolve_fabric_path(raw_path)
            try:
                raw_df = self.spark.read.option("recursiveFileLookup", "true").parquet(clean_path)
                self.merge_into_bronze(raw_df, table_name, pk)
            except Exception as e:
                logging.error(f"❌ Error procesando {table_name}: {e}")