import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, DateType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SilverProcessor:
    """Procesador para la capa Silver: Limpieza, tipado, normalización, desanidado y filtrado territorial."""

    # Bounding Box: España completa (Península, Canarias, Baleares, Ceuta, Melilla) + Búfer Fronterizo
    LAT_MIN = 27.0
    LAT_MAX = 44.0
    LON_MIN = -18.5
    LON_MAX = 5.0

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _filter_spain_with_buffer(self, df: DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> DataFrame:
        """Aplica un filtro geográfico para mantener solo el territorio español y zonas limítrofes."""
        return df.filter(
            (F.col(lat_col) >= self.LAT_MIN) & (F.col(lat_col) <= self.LAT_MAX) &
            (F.col(lon_col) >= self.LON_MIN) & (F.col(lon_col) <= self.LON_MAX)
        )

    def _save_to_silver(self, df: DataFrame, table_name: str, primary_keys: List[str]) -> None:
        if df is None or df.rdd.isEmpty():
            logging.warning(f"⚠️ DataFrame para '{table_name}' está vacío. Cancelando Silver.")
            return

        if "ingestion_timestamp" in df.columns:
            dedup_df = df.orderBy(F.col("ingestion_timestamp").desc()).dropDuplicates(subset=primary_keys)
        else:
            dedup_df = df.dropDuplicates(subset=primary_keys)

        if self.spark.catalog.tableExists(table_name):
            delta_table = DeltaTable.forName(self.spark, table_name)
            merge_cond = " AND ".join([f"target.{col} = source.{col}" for col in primary_keys])

            meta_cols = {"landing_source_file", "ingestion_timestamp", "updated_source_file", "updated_timestamp"}
            data_cols = [c for c in dedup_df.columns if c not in primary_keys and c not in meta_cols]

            update_cond = " OR ".join([f"NOT (target.{c} <=> source.{c})" for c in data_cols]) if data_cols else "1 = 0"

            insert_vals = {col: f"source.{col}" for col in dedup_df.columns if col not in ["updated_source_file", "updated_timestamp"]}
            insert_vals["updated_source_file"] = "CAST(NULL AS STRING)"
            insert_vals["updated_timestamp"] = "CAST(NULL AS TIMESTAMP)"

            update_vals = {col: f"source.{col}" for col in dedup_df.columns if col not in ["landing_source_file", "ingestion_timestamp"]}
            update_vals["landing_source_file"] = "target.landing_source_file"
            update_vals["ingestion_timestamp"] = "target.ingestion_timestamp"
            update_vals["updated_source_file"] = "source.landing_source_file"
            update_vals["updated_timestamp"] = "current_timestamp()"

            delta_table.alias("target") \
                .merge(dedup_df.alias("source"), merge_cond) \
                .whenMatchedUpdate(condition=update_cond, set=update_vals) \
                .whenNotMatchedInsert(values=insert_vals) \
                .execute()
            logging.info(f"✅ Tabla Silver Delta '{table_name}' actualizada mediante MERGE.")
        else:
            initial_df = dedup_df.selectExpr(
                "*",
                "CAST(NULL AS STRING) AS updated_source_file",
                "CAST(NULL AS TIMESTAMP) AS updated_timestamp"
            )
            initial_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)
            logging.info(f"✨ Tabla Silver Delta '{table_name}' creada por primera vez.")

    def process_silver_weather(self) -> None:
        try:
            raw_df = self.spark.table("bronze_weather")
            exploded_df = raw_df.select(
                F.col("latitude"),
                F.col("longitude"),
                F.col("elevation"),
                F.col("timezone"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp"),
                F.col("hourly"),
                F.posexplode(F.col("hourly.time")).alias("pos", "forecast_time_str")
            )

            weather_silver_df = exploded_df.select(
                F.col("latitude").cast(DoubleType()),
                F.col("longitude").cast(DoubleType()),
                F.col("elevation").cast(DoubleType()),
                F.col("timezone"),
                F.to_timestamp(F.col("forecast_time_str")).alias("forecast_timestamp"),
                F.element_at(F.col("hourly.temperature_2m"), F.col("pos") + 1).cast(DoubleType()).alias("temperature_celsius"),
                F.element_at(F.col("hourly.relative_humidity_2m"), F.col("pos") + 1).cast(IntegerType()).alias("humidity_percentage"),
                F.element_at(F.col("hourly.precipitation"), F.col("pos") + 1).cast(DoubleType()).alias("precipitation_mm"),
                F.element_at(F.col("hourly.rain"), F.col("pos") + 1).cast(DoubleType()).alias("rain_mm"),
                F.element_at(F.col("hourly.showers"), F.col("pos") + 1).cast(DoubleType()).alias("showers_mm"),
                F.element_at(F.col("hourly.snowfall"), F.col("pos") + 1).cast(DoubleType()).alias("snowfall_cm"),
                F.element_at(F.col("hourly.wind_speed_10m"), F.col("pos") + 1).cast(DoubleType()).alias("wind_speed_kmh"),
                F.element_at(F.col("hourly.wind_gusts_10m"), F.col("pos") + 1).cast(DoubleType()).alias("wind_gusts_kmh"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            )

            weather_filtered_df = self._filter_spain_with_buffer(weather_silver_df)
            self._save_to_silver(weather_filtered_df, "silver_weather", ["latitude", "longitude", "forecast_timestamp"])
        except Exception as e:
            logging.error(f"❌ Error procesando 'silver_weather': {e}")
            raise e

    def process_silver_nasa_fires(self) -> None:
        try:
            dfs_to_union = []
            if self.spark.catalog.tableExists("bronze_nasa_historical"):
                dfs_to_union.append(self.spark.table("bronze_nasa_historical"))
            if self.spark.catalog.tableExists("bronze_nasa_nrt"):
                dfs_to_union.append(self.spark.table("bronze_nasa_nrt"))

            if not dfs_to_union:
                logging.warning("⚠️ No existen tablas Bronze de NASA para procesar Silver.")
                return

            union_df = dfs_to_union[0]
            for next_df in dfs_to_union[1:]:
                union_df = union_df.unionByName(next_df, allowMissingColumns=True)

            formatted_time = F.lpad(F.col("acq_time").cast("string"), 4, "0")
            time_str = F.concat_ws(" ", F.col("acq_date").cast("string"), formatted_time)

            silver_fires_df = union_df.select(
                F.col("latitude").cast(DoubleType()),
                F.col("longitude").cast(DoubleType()),
                F.col("acq_date").cast(DateType()),
                F.col("acq_time").cast(IntegerType()),
                F.to_timestamp(time_str, "yyyy-MM-dd HHmm").alias("fire_detection_timestamp"),
                F.col("bright_ti4").cast(DoubleType()),
                F.col("bright_ti5").cast(DoubleType()),
                F.col("frp").cast(DoubleType()).alias("fire_radiative_power"),
                F.col("confidence"),
                F.col("daynight"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            )

            fires_filtered_df = self._filter_spain_with_buffer(silver_fires_df)
            self._save_to_silver(fires_filtered_df, "silver_nasa_fires", ["latitude", "longitude", "acq_date", "acq_time"])
        except Exception as e:
            logging.error(f"❌ Error procesando 'silver_nasa_fires': {e}")
            raise e

    def process_silver_dgt_traffic(self) -> None:
        try:
            raw_df = self.spark.table("bronze_dgt_traffic")

            silver_dgt_df = raw_df.select(
                F.col("record_id"),
                F.col("roadName").alias("road_name"),
                F.col("province"),
                F.col("autonomousCommunity").alias("autonomous_community"),
                F.col("municipality"),
                F.col("latitude").cast(DoubleType()),
                F.col("longitude").cast(DoubleType()),
                F.col("kilometerPoint").cast(DoubleType()).alias("kilometer_point"),
                F.col("causeType").alias("cause_type"),
                F.coalesce(F.col("roadMaintenanceType"), F.col("vehicleObstructionType"), F.col("environmentalObstructionType"), F.lit("unknown")).alias("incident_detail_type"),
                F.coalesce(F.col("severity"), F.lit("normal")).alias("severity_level"),
                F.to_timestamp(F.col("overallStartTime")).alias("start_timestamp"),
                F.to_timestamp(F.col("overallEndTime")).alias("end_timestamp"),
                F.col("carriageway"),
                F.col("laneUsage").alias("lane_usage"),
                F.col("vehicleType").alias("vehicle_type"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            ).filter(F.col("record_id").isNotNull())

            dgt_filtered_df = self._filter_spain_with_buffer(silver_dgt_df)
            self._save_to_silver(dgt_filtered_df, "silver_dgt_traffic", ["record_id"])
        except Exception as e:
            logging.error(f"❌ Error procesando 'silver_dgt_traffic': {e}")
            raise e

    def process_silver_osm_roads(self) -> None:
        try:
            raw_df = self.spark.table("bronze_osm_roads")

            silver_roads_df = raw_df.select(
                F.col("id").alias("osm_way_id"),
                F.col("nodes_count"),
                F.col("tag_highway").alias("road_classification"),
                F.col("tag_ref").alias("road_reference"),
                F.col("tag_name").alias("road_name"),
                F.col("tag_maxspeed").cast(IntegerType()).alias("max_speed_kmh"),
                F.col("tag_lanes").cast(IntegerType()).alias("lanes_count"),
                F.col("tag_oneway").alias("is_oneway"),
                F.col("tag_surface").alias("pavement_surface"),
                F.col("tag_bridge").alias("has_bridge"),
                F.col("tag_tunnel").alias("has_tunnel"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            ).filter(F.col("osm_way_id").isNotNull())

            self._save_to_silver(silver_roads_df, "silver_osm_roads", ["osm_way_id"])
        except Exception as e:
            logging.error(f"❌ Error procesando 'silver_osm_roads': {e}")
            raise e

    def run_all_silver_pipeline(self) -> None:
        logging.info("🚀 [SILVER] Iniciando pipeline de transformación de capa Bronze a Silver...")
        self.process_silver_weather()
        self.process_silver_nasa_fires()
        self.process_silver_dgt_traffic()
        self.process_silver_osm_roads()
        logging.info("✅ [SILVER OK] Pipeline Silver finalizado con éxito.")