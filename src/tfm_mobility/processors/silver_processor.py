import logging
from typing import List, Dict, Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    DateType
)
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable


# ============================================================
# LOGGING
# ============================================================

#configuracion estandar para monitorizar las transformaciones complejas de la capa silver
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class SilverProcessor:
    """
    Procesador optimizado de la capa Silver.

    Tablas generadas:
        - silver_weather
        - silver_nasa_fires
        - silver_fire_clusters
        - silver_dgt_traffic
        - silver_osm_roads
    """

    # ========================================================
    # ÁREA GEOGRÁFICA
    # ========================================================

    LAT_MIN = 27.0
    LAT_MAX = 44.0

    LON_MIN = -18.5
    LON_MAX = 5.0

    # ========================================================
    # CLUSTERING DE INCENDIOS
    # ========================================================

    FIRE_CLUSTER_DISTANCE_KM = 5.0
    FIRE_CLUSTER_TIME_HOURS = 24

    SPATIAL_CELL_LAT_DEG = 0.05
    SPATIAL_CELL_LON_DEG = 0.07

    # ========================================================
    # CONSTRUCTOR
    # ========================================================

    def __init__(self, spark: SparkSession):

        #aqui ajusto la sesion de spark activando el auto merge y la ejecucion adaptativa para optimizar los joins espaciales
        self.spark = spark

        self.spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true"
        )

        self.spark.conf.set(
            "spark.sql.adaptive.enabled",
            "true"
        )

        self.spark.conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        self.spark.conf.set(
            "spark.sql.adaptive.skewJoin.enabled",
            "true"
        )

    # ========================================================
    # UTILIDADES
    # ========================================================

    def _filter_spain_with_buffer(
        self,
        df: DataFrame,
        lat_col: str = "latitude",
        lon_col: str = "longitude"
    ) -> DataFrame:

        #he programado este filtro para descartar datos que caigan fuera de la ventana geografica de españa
        return df.filter(
            F.col(lat_col).isNotNull()
            & F.col(lon_col).isNotNull()
            & (F.col(lat_col) >= self.LAT_MIN)
            & (F.col(lat_col) <= self.LAT_MAX)
            & (F.col(lon_col) >= self.LON_MIN)
            & (F.col(lon_col) <= self.LON_MAX)
        )

    def _calculate_distance_km(
        self,
        lat1,
        lon1,
        lat2,
        lon2
    ):

        #aplicacion de la formula de haversine en pyspark para calcular la distancia geografica real entre dos puntos
        return (
            6371.0
            * 2.0
            * F.asin(
                F.sqrt(
                    F.pow(
                        F.sin(
                            F.radians(lat2 - lat1) / 2.0
                        ),
                        2
                    )
                    +
                    F.cos(F.radians(lat1))
                    * F.cos(F.radians(lat2))
                    * F.pow(
                        F.sin(
                            F.radians(lon2 - lon1) / 2.0
                        ),
                        2
                    )
                )
            )
        )

    def _save_to_silver(
        self,
        df: DataFrame,
        table_name: str,
        primary_keys: List[str]
    ) -> None:

        #verifico que el dataframe contenga registros antes de actualizar la capa silver
        if df is None:
            logging.warning(
                f"DataFrame '{table_name}' es None."
            )
            return

        if not df.take(1):
            logging.warning(
                f"DataFrame '{table_name}' esta vacio."
            )
            return

        #aqui aplico una funcion de ventana para eliminar duplicados quedandome con la ultima marca de ingesta
        if "ingestion_timestamp" in df.columns:

            from pyspark.sql.window import Window

            window_spec = (
                Window
                .partitionBy(*primary_keys)
                .orderBy(
                    F.col("ingestion_timestamp").desc_nulls_last()
                )
            )

            dedup_df = (
                df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

        else:

            dedup_df = df.dropDuplicates(primary_keys)

        #estampado de los campos de trazabilidad para auditar cuando se modifico el dato
        source_df = (
            dedup_df
            .withColumn(
                "updated_source_file",
                F.col("landing_source_file")
            )
            .withColumn(
                "updated_timestamp",
                F.current_timestamp()
            )
        )

        #si la tabla delta existe en el catalogo se realiza una operacion de merge condicional
        if self.spark.catalog.tableExists(table_name):

            delta_table = DeltaTable.forName(
                self.spark,
                table_name
            )

            merge_condition = " AND ".join(
                [
                    f"target.`{col}` = source.`{col}`"
                    for col in primary_keys
                ]
            )

            (
                delta_table
                .alias("target")
                .merge(
                    source_df.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            logging.info(
                f"'{table_name}' actualizado mediante MERGE."
            )

        else:

            #creacion inicial de la tabla delta si es la primera vez que se ejecuta la canalizacion
            (
                source_df
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
            )

            logging.info(
                f"'{table_name}' creada."
            )

    # ============================================================
    # WEATHER
    # ============================================================

    def process_silver_weather(self) -> None:

        try:

            logging.info(
                "Procesando silver_weather..."
            )

            self.spark.catalog.refreshTable(
                "bronze_weather"
            )

            raw_df = self.spark.table(
                "bronze_weather"
            )

            #he utilizado posexplode para desanidar los vectores de las predicciones meteorologicas manteniendo sincronizadas las posiciones
            exploded_df = (
                raw_df
                .select(
                    F.col("latitude"),
                    F.col("longitude"),
                    F.col("elevation"),
                    F.col("timezone"),
                    F.col("landing_source_file"),
                    F.col("ingestion_timestamp"),
                    F.posexplode(
                        F.col("hourly.time")
                    ).alias(
                        "pos",
                        "forecast_time_str"
                    ),
                    F.col("hourly")
                )
                .select(
                    F.col("latitude")
                    .cast(DoubleType())
                    .alias("latitude"),

                    F.col("longitude")
                    .cast(DoubleType())
                    .alias("longitude"),

                    F.col("elevation")
                    .cast(DoubleType())
                    .alias("elevation"),

                    F.col("timezone"),

                    F.to_timestamp(
                        F.col("forecast_time_str")
                    ).alias(
                        "forecast_timestamp"
                    ),

                    #extraccion escalar sincronizada usando la posicion pos mas uno
                    F.element_at(
                        F.col("hourly.temperature_2m"),
                        F.col("pos") + 1
                    )
                    .cast(DoubleType())
                    .alias(
                        "temperature_celsius"
                    ),

                    F.element_at(
                        F.col("hourly.relative_humidity_2m"),
                        F.col("pos") + 1
                    )
                    .cast(IntegerType())
                    .alias(
                        "humidity_percentage"
                    ),

                    F.element_at(
                        F.col("hourly.precipitation"),
                        F.col("pos") + 1
                    )
                    .cast(DoubleType())
                    .alias(
                        "precipitation_mm"
                    ),

                    F.element_at(
                        F.col("hourly.wind_speed_10m"),
                        F.col("pos") + 1
                    )
                    .cast(DoubleType())
                    .alias(
                        "wind_speed_kmh"
                    ),

                    F.col("landing_source_file"),

                    F.col("ingestion_timestamp")
                )
            )

            weather_filtered_df = (
                self._filter_spain_with_buffer(
                    exploded_df
                )
            )

            self._save_to_silver(
                weather_filtered_df,
                "silver_weather",
                [
                    "latitude",
                    "longitude",
                    "forecast_timestamp"
                ]
            )

            logging.info(
                "silver_weather completado."
            )

        except Exception as e:

            logging.error(
                f"Error en silver_weather: {e}"
            )

            raise

    # ============================================================
    # NASA FIRES
    # ============================================================

    def process_silver_nasa_fires(self) -> None:

        try:

            logging.info(
                "Procesando silver_nasa_fires..."
            )

            #he construido esta union combinando el historico de 7 dias con los datos nrt de tiempo real
            dfs_to_union = []

            if self.spark.catalog.tableExists(
                "bronze_nasa_historical"
            ):
                dfs_to_union.append(
                    self.spark.table(
                        "bronze_nasa_historical"
                    )
                )

            if self.spark.catalog.tableExists(
                "bronze_nasa_nrt"
            ):
                dfs_to_union.append(
                    self.spark.table(
                        "bronze_nasa_nrt"
                    )
                )

            if not dfs_to_union:

                logging.warning(
                    "No existen tablas Bronze NASA."
                )

                return

            union_df = dfs_to_union[0]

            for next_df in dfs_to_union[1:]:

                union_df = union_df.unionByName(
                    next_df,
                    allowMissingColumns=True
                )

            #aqui formateo la hora rellenando con ceros a la izquierda para poder construir una marca de tiempo valida
            formatted_time = F.lpad(
                F.col("acq_time").cast("string"),
                4,
                "0"
            )

            time_str = F.concat_ws(
                " ",
                F.col("acq_date").cast("string"),
                formatted_time
            )

            silver_fires_df = (
                union_df
                .select(
                    F.col("latitude")
                    .cast(DoubleType())
                    .alias("latitude"),

                    F.col("longitude")
                    .cast(DoubleType())
                    .alias("longitude"),

                    F.col("acq_date")
                    .cast(DateType())
                    .alias("acq_date"),

                    F.col("acq_time")
                    .cast(IntegerType())
                    .alias("acq_time"),

                    F.to_timestamp(
                        time_str,
                        "yyyy-MM-dd HHmm"
                    ).alias(
                        "fire_detection_timestamp"
                    ),

                    F.col("bright_ti4")
                    .cast(DoubleType())
                    .alias("bright_ti4"),

                    F.col("bright_ti5")
                    .cast(DoubleType())
                    .alias("bright_ti5"),

                    F.col("frp")
                    .cast(DoubleType())
                    .alias(
                        "fire_radiative_power"
                    ),

                    F.col("confidence"),

                    F.col("daynight"),

                    F.col("landing_source_file"),

                    F.col("ingestion_timestamp")
                )
            )

            fires_filtered_df = (
                self._filter_spain_with_buffer(
                    silver_fires_df
                )
            )

            #deduplicacion mediante funciones de ventana para eliminar alertas duplicadas en pasadas de satelite
            from pyspark.sql.window import Window

            window_spec = (
                Window
                .partitionBy(
                    "latitude",
                    "longitude",
                    "acq_date",
                    "acq_time"
                )
                .orderBy(
                    F.col(
                        "ingestion_timestamp"
                    ).desc_nulls_last()
                )
            )

            fires = (
                fires_filtered_df
                .withColumn(
                    "_rn",
                    F.row_number().over(
                        window_spec
                    )
                )
                .filter(
                    F.col("_rn") == 1
                )
                .drop("_rn")
                .filter(
                    F.col(
                        "fire_detection_timestamp"
                    ).isNotNull()
                )
                .withColumn(
                    "fire_id",
                    F.sha2(
                        F.concat_ws(
                            "|",
                            F.col("latitude").cast("string"),
                            F.col("longitude").cast("string"),
                            F.col(
                                "fire_detection_timestamp"
                            ).cast("string")
                        ),
                        256
                    )
                )
            )

            #persisto el dataframe en memoria y disco para agilizar la ejecucion del clustering posterior
            fires = fires.persist(
                StorageLevel.MEMORY_AND_DISK
            )

            if not fires.take(1):

                logging.warning(
                    "No hay detecciones NASA validas."
                )

                fires.unpersist()

                return

            #aqui invoco la funcion que resuelve el agrupamiento espacial y temporal de los focos de fuego
            cluster_mapping = (
                self._build_fire_clusters(
                    fires
                )
            )

            enriched_fires = (
                fires
                .join(
                    cluster_mapping,
                    "fire_id",
                    "left"
                )
            )

            final_nasa_fires = (
                enriched_fires
                .select(
                    "latitude",
                    "longitude",
                    "acq_date",
                    "acq_time",
                    "fire_detection_timestamp",
                    "bright_ti4",
                    "bright_ti5",
                    "fire_radiative_power",
                    "confidence",
                    "daynight",
                    "landing_source_file",
                    "ingestion_timestamp",
                    "cluster_id"
                )
                .withColumn(
                    "updated_source_file",
                    F.col("landing_source_file")
                )
                .withColumn(
                    "updated_timestamp",
                    F.current_timestamp()
                )
                .dropDuplicates(
                    [
                        "latitude",
                        "longitude",
                        "acq_date",
                        "acq_time"
                    ]
                )
            )

            (
                final_nasa_fires
                .write
                .format("delta")
                .mode("overwrite")
                .option(
                    "overwriteSchema",
                    "true"
                )
                .saveAsTable(
                    "silver_nasa_fires"
                )
            )

            logging.info(
                "silver_nasa_fires creada/actualizada."
            )

            #se genera un resumen a nivel de cluster agregando estadisticas de potencia y centroides
            cluster_summary = (
                enriched_fires
                .groupBy(
                    "cluster_id"
                )
                .agg(
                    F.min(
                        "fire_detection_timestamp"
                    ).alias(
                        "first_detection_timestamp"
                    ),

                    F.max(
                        "fire_detection_timestamp"
                    ).alias(
                        "last_detection_timestamp"
                    ),

                    F.countDistinct(
                        "fire_id"
                    ).alias(
                        "detection_count"
                    ),

                    F.min(
                        "latitude"
                    ).alias(
                        "lat_min"
                    ),

                    F.max(
                        "latitude"
                    ).alias(
                        "lat_max"
                    ),

                    F.min(
                        "longitude"
                    ).alias(
                        "lon_min"
                    ),

                    F.max(
                        "longitude"
                    ).alias(
                        "lon_max"
                    ),

                    F.avg(
                        "latitude"
                    ).alias(
                        "centroid_latitude"
                    ),

                    F.avg(
                        "longitude"
                    ).alias(
                        "centroid_longitude"
                    ),

                    F.max(
                        "fire_radiative_power"
                    ).alias(
                        "max_fire_radiative_power"
                    ),

                    F.avg(
                        "fire_radiative_power"
                    ).alias(
                        "avg_fire_radiative_power"
                    )
                )
                .withColumn(
                    "updated_timestamp",
                    F.current_timestamp()
                )
            )

            (
                cluster_summary
                .write
                .format("delta")
                .mode("overwrite")
                .option(
                    "overwriteSchema",
                    "true"
                )
                .saveAsTable(
                    "silver_fire_clusters"
                )
            )

            fires.unpersist()

            logging.info(
                "Clustering y silver_fire_clusters completados."
            )

        except Exception as e:

            logging.error(
                f"Error en silver_nasa_fires: {e}"
            )

            raise

    # ============================================================
    # FIRE CLUSTERING
    # ============================================================

    def _build_fire_clusters(
        self,
        fires: DataFrame
    ) -> DataFrame:

        logging.info("Construyendo clusters de incendios...")

        #se asigna cada deteccion a celdas temporales y espaciales fijas para acotar las comparaciones
        nodes_df = (
            fires
            .select("fire_id", "latitude", "longitude", "fire_detection_timestamp")
            .dropDuplicates(["fire_id"])
            .withColumn(
                "_lat_cell",
                F.floor(F.col("latitude") / F.lit(self.SPATIAL_CELL_LAT_DEG)).cast("long")
            )
            .withColumn(
                "_lon_cell",
                F.floor(F.col("longitude") / F.lit(self.SPATIAL_CELL_LON_DEG)).cast("long")
            )
            .withColumn(
                "_time_bucket",
                F.floor(
                    F.unix_timestamp("fire_detection_timestamp")
                    / F.lit(self.FIRE_CLUSTER_TIME_HOURS * 3600)
                ).cast("long")
            )
        )

        #definicion de los 27 desplazamientos adyacentes para evaluar unicamente las celdas colindantes
        offsets = [
            (-1, -1, -1), (-1, -1, 0), (-1, -1, 1),
            (-1, 0, -1),  (-1, 0, 0),  (-1, 0, 1),
            (-1, 1, -1),  (-1, 1, 0),  (-1, 1, 1),
            (0, -1, -1),  (0, -1, 0),  (0, -1, 1),
            (0, 0, -1),   (0, 0, 0),   (0, 0, 1),
            (0, 1, -1),   (0, 1, 0),   (0, 1, 1),
            (1, -1, -1),  (1, -1, 0),  (1, -1, 1),
            (1, 0, -1),   (1, 0, 0),   (1, 0, 1),
            (1, 1, -1),   (1, 1, 0),   (1, 1, 1)
        ]

        offsets_df = self.spark.createDataFrame(
            offsets,
            ["_lat_offset", "_lon_offset", "_time_offset"]
        )

        #se cruzan los candidatos utilizando broadcast para no saturar la red del cluster
        candidates = (
            nodes_df
            .crossJoin(F.broadcast(offsets_df))
            .withColumn("_join_lat_cell", F.col("_lat_cell") + F.col("_lat_offset"))
            .withColumn("_join_lon_cell", F.col("_lon_cell") + F.col("_lon_offset"))
            .withColumn("_join_time_bucket", F.col("_time_bucket") + F.col("_time_offset"))
        )

        a = nodes_df.select(
            F.col("fire_id").alias("fire_id_a"),
            F.col("latitude").alias("latitude_a"),
            F.col("longitude").alias("longitude_a"),
            F.col("fire_detection_timestamp").alias("timestamp_a"),
            F.col("_lat_cell").alias("join_lat_cell"),
            F.col("_lon_cell").alias("join_lon_cell"),
            F.col("_time_bucket").alias("join_time_bucket")
        )

        b = candidates.select(
            F.col("fire_id").alias("fire_id_b"),
            F.col("latitude").alias("latitude_b"),
            F.col("longitude").alias("longitude_b"),
            F.col("fire_detection_timestamp").alias("timestamp_b"),
            F.col("_join_lat_cell").alias("join_lat_cell"),
            F.col("_join_lon_cell").alias("join_lon_cell"),
            F.col("_join_time_bucket").alias("join_time_bucket")
        )

        #se calcula la distancia de haversine y la diferencia de tiempo sobre los candidatos filtrados
        candidate_edges = (
            a.join(
                b,
                on=["join_lat_cell", "join_lon_cell", "join_time_bucket"],
                how="inner"
            )
            .filter(F.col("fire_id_a") < F.col("fire_id_b"))
            .withColumn(
                "distance_km",
                self._calculate_distance_km(
                    F.col("latitude_a"), F.col("longitude_a"),
                    F.col("latitude_b"), F.col("longitude_b")
                )
            )
            .withColumn(
                "time_difference_seconds",
                F.abs(F.unix_timestamp("timestamp_b") - F.unix_timestamp("timestamp_a"))
            )
            .filter(F.col("distance_km") <= self.FIRE_CLUSTER_DISTANCE_KM)
            .filter(F.col("time_difference_seconds") <= self.FIRE_CLUSTER_TIME_HOURS * 3600)
            .select("fire_id_a", "fire_id_b")
            .dropDuplicates(["fire_id_a", "fire_id_b"])
        )

        logging.info("Obteniendo lista de conexiones de incendios...")

        #extraigo la lista reducida de aristas al driver para resolver las componentes conexas en memoria local
        all_fire_ids = [row["fire_id"] for row in nodes_df.select("fire_id").collect()]
        edges_list = candidate_edges.collect()

        adj: Dict[str, List[str]] = {fid: [] for fid in all_fire_ids}
        for row in edges_list:
            u, v = row["fire_id_a"], row["fire_id_b"]
            adj[u].append(v)
            adj[v].append(u)

        visited: Set[str] = set()
        mapping_data = []

        #algoritmo de busqueda en profundidad para asignar un cluster_id unico a cada componente conectada
        for fid in all_fire_ids:
            if fid not in visited:
                component = []
                stack = [fid]
                visited.add(fid)
                while stack:
                    curr = stack.pop()
                    component.append(curr)
                    for neighbor in adj[curr]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            stack.append(neighbor)

                cluster_root = sorted(component)[0]
                cluster_id = f"INC_{cluster_root[:12]}"
                for item in component:
                    mapping_data.append((item, cluster_id))

        #he reconvertido el mapeo de clusters a dataframe de spark para unirlo con las detecciones originales
        cluster_mapping = self.spark.createDataFrame(
            mapping_data,
            ["fire_id", "cluster_id"]
        )

        logging.info("Clusters de incendios generados correctamente.")

        return cluster_mapping

    # ============================================================
    # DGT TRAFFIC
    # ============================================================

    def process_silver_dgt_traffic(self) -> None:

        try:

            logging.info(
                "Procesando silver_dgt_traffic..."
            )

            raw_df = self.spark.table(
                "bronze_dgt_traffic"
            )

            #aqui selecciono y normalizo los campos de las incidencias de trafico de la dgt
            silver_dgt_df = (
                raw_df
                .select(
                    F.col("record_id")
                    .cast("string")
                    .alias("record_id"),

                    F.col("roadName")
                    .alias("road_name"),

                    F.col("province"),

                    F.col("autonomousCommunity")
                    .alias(
                        "autonomous_community"
                    ),

                    F.col("municipality"),

                    F.col("latitude")
                    .cast(DoubleType())
                    .alias("latitude"),

                    F.col("longitude")
                    .cast(DoubleType())
                    .alias("longitude"),

                    F.col("kilometerPoint")
                    .cast(DoubleType())
                    .alias(
                        "kilometer_point"
                    ),

                    F.col("causeType")
                    .alias("cause_type"),

                    F.coalesce(
                        F.col(
                            "roadMaintenanceType"
                        ),
                        F.col(
                            "vehicleObstructionType"
                        ),
                        F.col(
                            "environmentalObstructionType"
                        ),
                        F.lit("unknown")
                    ).alias(
                        "incident_detail_type"
                    ),

                    F.coalesce(
                        F.col("severity"),
                        F.lit("normal")
                    ).alias(
                        "severity_level"
                    ),

                    F.to_timestamp(
                        F.col(
                            "overallStartTime"
                        )
                    ).alias(
                        "start_timestamp"
                    ),

                    F.to_timestamp(
                        F.col(
                            "overallEndTime"
                        )
                    ).alias(
                        "end_timestamp"
                    ),

                    F.col("carriageway"),

                    F.col("laneUsage")
                    .alias("lane_usage"),

                    F.col("vehicleType")
                    .alias("vehicle_type"),

                    F.col(
                        "landing_source_file"
                    ),

                    F.to_timestamp(
                        F.col(
                            "ingestion_timestamp"
                        )
                    ).alias(
                        "ingestion_timestamp"
                    )
                )
                .filter(
                    F.col(
                        "record_id"
                    ).isNotNull()
                )
            )

            dgt_filtered_df = (
                self._filter_spain_with_buffer(
                    silver_dgt_df
                )
            )

            self._save_to_silver(
                dgt_filtered_df,
                "silver_dgt_traffic",
                ["record_id"]
            )

            #he implementado este bloque para cerrar el ciclo de vida marcando end_timestamp cuando una incidencia deja de aparecer en las ingestas
            if self.spark.catalog.tableExists(
                "silver_dgt_traffic"
            ):

                latest_ingestion = (
                    dgt_filtered_df
                    .agg(
                        F.max(
                            "ingestion_timestamp"
                        ).alias(
                            "latest_ingestion"
                        )
                    )
                    .collect()[0]["latest_ingestion"]
                )

                if latest_ingestion is not None:

                    current_ids = (
                        dgt_filtered_df
                        .select(
                            "record_id"
                        )
                        .distinct()
                    )

                    delta_table = DeltaTable.forName(
                        self.spark,
                        "silver_dgt_traffic"
                    )

                    #aqui utilizo whennotmatchedbysourceupdate para marcar como finalizada la incidencia si ya no viene en el feed
                    (
                        delta_table
                        .alias("target")
                        .merge(
                            current_ids.alias("source"),
                            "target.record_id = source.record_id"
                        )
                        .whenNotMatchedBySourceUpdate(
                            condition=(
                                F.col(
                                    "target.end_timestamp"
                                ).isNull()
                                &
                                (
                                    F.col(
                                        "target.ingestion_timestamp"
                                    )
                                    <
                                    F.lit(
                                        latest_ingestion
                                    )
                                )
                            ),
                            set={
                                "end_timestamp":
                                    F.lit(
                                        latest_ingestion
                                    ),

                                "updated_timestamp":
                                    F.current_timestamp()
                            }
                        )
                        .execute()
                    )

            logging.info(
                "silver_dgt_traffic completado."
            )

        except Exception as e:

            logging.error(
                f"Error en silver_dgt_traffic: {e}"
            )

            raise

    # ============================================================
    # OSM ROADS (CON APLANAMIENTO DE GEOMETRÍA EN CENTROIDE)
    # ============================================================

    def process_silver_osm_roads(self) -> None:

        try:

            logging.info(
                "Procesando silver_osm_roads..."
            )

            if self.spark.catalog.tableExists("bronze_osm_roads"):
                self.spark.catalog.refreshTable("bronze_osm_roads")
            else:
                logging.warning("La tabla 'bronze_osm_roads' no existe en Bronze todavia. Omitiendo Silver OSM.")
                return

            raw_df = self.spark.table(
                "bronze_osm_roads"
            )

            has_geom = (
                "geometry_json"
                in raw_df.columns
            )

            #extraigo las coordenadas escalares del primer punto de la linea usando get_json_object para simplificar consultas
            if has_geom:
                geom_col = F.col("geometry_json")
                centroid_lat = F.get_json_object(F.col("geometry_json"), "$[0].lat").cast(DoubleType())
                centroid_lon = F.get_json_object(F.col("geometry_json"), "$[0].lon").cast(DoubleType())
            else:
                geom_col = F.lit(None).cast("string")
                centroid_lat = F.lit(None).cast(DoubleType())
                centroid_lon = F.lit(None).cast(DoubleType())

            silver_roads_df = (
                raw_df
                .select(
                    F.col("id")
                    .alias("osm_way_id"),

                    F.col("nodes_count"),

                    F.col("tag_highway")
                    .alias(
                        "road_classification"
                    ),

                    F.col("tag_ref")
                    .alias(
                        "road_reference"
                    ),

                    F.col("tag_name")
                    .alias(
                        "road_name"
                    ),

                    F.col("tag_maxspeed")
                    .cast(IntegerType())
                    .alias(
                        "max_speed_kmh"
                    ),

                    F.col("tag_lanes")
                    .cast(IntegerType())
                    .alias(
                        "lanes_count"
                    ),

                    F.col("tag_oneway")
                    .alias(
                        "is_oneway"
                    ),

                    F.col("tag_surface")
                    .alias(
                        "pavement_surface"
                    ),

                    F.col("tag_bridge")
                    .alias(
                        "has_bridge"
                    ),

                    F.col("tag_tunnel")
                    .alias(
                        "has_tunnel"
                    ),

                    centroid_lat.alias("centroid_latitude"),
                    centroid_lon.alias("centroid_longitude"),

                    geom_col.alias(
                        "road_geometry"
                    ),

                    F.col(
                        "landing_source_file"
                    ),

                    F.col(
                        "ingestion_timestamp"
                    )
                )
                .filter(
                    F.col(
                        "osm_way_id"
                    ).isNotNull()
                )
            )

            #si detecto una tabla desactualizada sin la columna centroid_latitude la recreo de forma limpia
            if self.spark.catalog.tableExists("silver_osm_roads"):
                existing_cols = self.spark.table("silver_osm_roads").columns
                if "centroid_latitude" not in existing_cols:
                    logging.warning("Detectado esquema antiguo sin centroid_latitude en silver_osm_roads. Recreando tabla...")
                    self.spark.sql("DROP TABLE IF EXISTS silver_osm_roads")

            self._save_to_silver(
                silver_roads_df,
                "silver_osm_roads",
                ["osm_way_id"]
            )

            logging.info(
                "silver_osm_roads completado con geometria aplanada."
            )

        except Exception as e:

            logging.error(
                f"Error en silver_osm_roads: {e}"
            )

            raise

    # ============================================================
    # PIPELINE COMPLETO
    # ============================================================

    def run_all_silver_pipeline(self) -> None:

        #metodo de orquestacion principal para ejecutar la transformacion completa de la capa silver
        logging.info(
            "[SILVER] Iniciando pipeline..."
        )

        try:

            self.process_silver_weather()
            self.process_silver_nasa_fires()
            self.process_silver_dgt_traffic()
            self.process_silver_osm_roads()

            logging.info(
                "========================================"
            )

            logging.info(
                "Pipeline finalizado."
            )

            logging.info(
                "========================================"
            )

        except Exception as e:

            logging.error(
                "========================================"
            )

            logging.error(
                f"[SILVER ERROR] {e}"
            )

            logging.error(
                "========================================"
            )

            raise