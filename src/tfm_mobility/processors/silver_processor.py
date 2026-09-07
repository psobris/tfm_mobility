import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SilverProcessor:
    """Procesador para la capa Silver: Limpieza, tipado, normalización, clústeres espaciales y filtrado territorial."""

    LAT_MIN = 27.0
    LAT_MAX = 44.0
    LON_MIN = -18.5
    LON_MAX = 5.0

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _filter_spain_with_buffer(self, df: DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> DataFrame:
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
            initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            logging.info(f"✨ Tabla Silver Delta '{table_name}' creada por primera vez.")

    def process_silver_weather(self) -> None:
        try:
            self.spark.catalog.refreshTable("bronze_weather")
            raw_df = self.spark.table("bronze_weather")

            exploded_df = raw_df.select(
                F.col("latitude"),
                F.col("longitude"),
                F.col("elevation"),
                F.col("timezone"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp"),
                F.posexplode(F.col("hourly.time")).alias("pos", "forecast_time_str"),
                F.col("hourly")
            ).select(
                F.col("latitude").cast(DoubleType()),
                F.col("longitude").cast(DoubleType()),
                F.col("elevation").cast(DoubleType()),
                F.col("timezone"),
                F.to_timestamp(F.col("forecast_time_str")).alias("forecast_timestamp"),
                F.element_at(F.col("hourly.temperature_2m"), F.col("pos") + 1).cast(DoubleType()).alias("temperature_celsius"),
                F.element_at(F.col("hourly.relative_humidity_2m"), F.col("pos") + 1).cast(IntegerType()).alias("humidity_percentage"),
                F.element_at(F.col("hourly.precipitation"), F.col("pos") + 1).cast(DoubleType()).alias("precipitation_mm"),
                F.element_at(F.col("hourly.wind_speed_10m"), F.col("pos") + 1).cast(DoubleType()).alias("wind_speed_kmh"),
                F.col("landing_source_file"),
                F.col("ingestion_timestamp")
            )

            weather_filtered_df = self._filter_spain_with_buffer(exploded_df)
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

            # Procesar clústeres dinámicos
            self.process_silver_fire_clusters()

        except Exception as e:
            logging.error(f"❌ Error procesando 'silver_nasa_fires': {e}")
            raise e

    def process_silver_fire_clusters(self) -> None:
        """
        Crea/actualiza la tabla 'silver_fire_clusters' ampliando sus límites (bounding box)
        y asigna el 'cluster_id' a cada registro de 'silver_nasa_fires'.
        """
        try:
            BUFFER_DEG = 0.045  # ~5 km en latitud/longitud
            
            if not self.spark.catalog.tableExists("silver_nasa_fires"):
                logging.warning("⚠️ La tabla 'silver_nasa_fires' no existe para calcular clústeres.")
                return

            raw_fires_df = self.spark.table("silver_nasa_fires")
            if raw_fires_df.rdd.isEmpty():
                return

            # 1. Crear la tabla de clústeres si no existe
            if not self.spark.catalog.tableExists("silver_fire_clusters"):
                initial_clusters = raw_fires_df.groupBy(
                    F.round(F.col("latitude"), 2).alias("grid_lat"),
                    F.round(F.col("longitude"), 2).alias("grid_lon")
                ).agg(
                    F.concat(F.lit("INC_"), F.format_string("%.2f", F.first("latitude")), F.lit("_"), F.format_string("%.2f", F.first("longitude"))).alias("cluster_id"),
                    F.min("latitude").alias("lat_min"),
                    F.max("latitude").alias("lat_max"),
                    F.min("longitude").alias("lon_min"),
                    F.max("longitude").alias("lon_max"),
                    F.min("fire_detection_timestamp").alias("first_detection_timestamp"),
                    F.max("fire_detection_timestamp").alias("last_detection_timestamp")
                ).drop("grid_lat", "grid_lon")

                initial_clusters.write.format("delta").mode("overwrite").saveAsTable("silver_fire_clusters")
                logging.info("✨ Tabla 'silver_fire_clusters' creada por primera vez.")

            # 2. Asignar cluster_id por proximidad a la bounding box ampliada
            clusters_df = self.spark.table("silver_fire_clusters")

            enriched_fires = raw_fires_df.alias("f").join(
                clusters_df.alias("c"),
                (F.col("f.latitude") >= (F.col("c.lat_min") - BUFFER_DEG)) &
                (F.col("f.latitude") <= (F.col("c.lat_max") + BUFFER_DEG)) &
                (F.col("f.longitude") >= (F.col("c.lon_min") - BUFFER_DEG)) &
                (F.col("f.longitude") <= (F.col("c.lon_max") + BUFFER_DEG)),
                "left"
            )

            enriched_fires = enriched_fires.withColumn(
                "assigned_cluster_id",
                F.coalesce(
                    F.col("c.cluster_id"),
                    F.concat(F.lit("INC_"), F.format_string("%.2f", F.col("f.latitude")), F.lit("_"), F.format_string("%.2f", F.col("f.longitude")))
                )
            )

            # 3. Recalcular límites de clústeres
            updated_clusters = enriched_fires.groupBy("assigned_cluster_id").agg(
                F.min("latitude").alias("new_lat_min"),
                F.max("latitude").alias("new_lat_max"),
                F.min("longitude").alias("new_lon_min"),
                F.max("longitude").alias("new_lon_max"),
                F.min("fire_detection_timestamp").alias("new_first_seen"),
                F.max("fire_detection_timestamp").alias("new_last_seen")
            )

            # 4. MERGE en 'silver_fire_clusters'
            delta_clusters = DeltaTable.forName(self.spark, "silver_fire_clusters")
            delta_clusters.alias("target").merge(
                updated_clusters.alias("source"),
                "target.cluster_id = source.assigned_cluster_id"
            ).whenMatchedUpdate(set={
                "lat_min": F.least(F.col("target.lat_min"), F.col("source.new_lat_min")),
                "lat_max": F.greatest(F.col("target.lat_max"), F.col("source.new_lat_max")),
                "lon_min": F.least(F.col("target.lon_min"), F.col("source.new_lon_min")),
                "lon_max": F.greatest(F.col("target.lon_max"), F.col("source.new_lon_max")),
                "last_detection_timestamp": F.greatest(F.col("target.last_detection_timestamp"), F.col("source.new_last_seen"))
            }).whenNotMatchedInsert(values={
                "cluster_id": "source.assigned_cluster_id",
                "lat_min": "source.new_lat_min",
                "lat_max": "source.new_lat_max",
                "lon_min": "source.new_lon_min",
                "lon_max": "source.new_lon_max",
                "first_detection_timestamp": "source.new_first_seen",
                "last_detection_timestamp": "source.new_last_seen"
            }).execute()

            # 5. Guardar 'silver_nasa_fires' enriquecida con cluster_id
            final_nasa_fires = enriched_fires.select(
                "latitude", "longitude", "acq_date", "acq_time", "fire_detection_timestamp",
                "bright_ti4", "bright_ti5", "fire_radiative_power", "confidence", "daynight",
                "landing_source_file", "ingestion_timestamp",
                F.col("assigned_cluster_id").alias("cluster_id")
            ).dropDuplicates(subset=["latitude", "longitude", "acq_date", "acq_time"])

            final_nasa_fires.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_nasa_fires")
            logging.info("✅ Clústeres de incendios y 'silver_nasa_fires' sincronizados.")

        except Exception as e:
            logging.error(f"❌ Error en process_silver_fire_clusters: {e}")
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
                F.to_timestamp(F.col("ingestion_timestamp")).alias("ingestion_timestamp")
            ).filter(F.col("record_id").isNotNull())

            dgt_filtered_df = self._filter_spain_with_buffer(silver_dgt_df)

            # 1. MERGE incremental
            self._save_to_silver(dgt_filtered_df, "silver_dgt_traffic", ["record_id"])

            # 2. Cierre automático de incidencias desaparecidas (Soft Delete)
            if not dgt_filtered_df.rdd.isEmpty() and self.spark.catalog.tableExists("silver_dgt_traffic"):
                active_ids = [row.record_id for row in dgt_filtered_df.select("record_id").distinct().collect()]
                latest_ingestion = dgt_filtered_df.select(F.max("ingestion_timestamp")).collect()[0][0]

                if active_ids and latest_ingestion:
                    delta_table = DeltaTable.forName(self.spark, "silver_dgt_traffic")
                    delta_table.alias("target").update(
                        condition=(
                            F.col("target.end_timestamp").isNull() & 
                            (~F.col("target.record_id").isin(active_ids))
                        ),
                        set={
                            "end_timestamp": F.lit(latest_ingestion),
                            "updated_timestamp": F.current_timestamp()
                        }
                    )
                    logging.info("✅ Cierre automático (Soft Delete) completado para incidencias DGT resueltas.")

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