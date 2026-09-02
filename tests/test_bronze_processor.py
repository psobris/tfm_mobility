import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, element_at, split

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class BronzeProcessor:
    """
    Procesador de capa Bronze. Parsea los archivos de la capa RAW
    y promociona los datos a Tablas Delta Bronze con metadatos de trazabilidad.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process_weather_to_bronze(self, raw_weather_path: str, bronze_table_name: str = "bronze_weather"):
        """
        Lee el Parquet de RAW (que contiene el array de respuestas por lotes/batching)
        y lo guarda/actualiza en la tabla Delta Bronze.
        """
        logging.info(f"🔄 Procesando datos meteorológicos de RAW ({raw_weather_path}) a Bronze ({bronze_table_name})...")

        df_raw = self.spark.read.parquet(raw_weather_path)

        # Si el Parquet contiene un array de respuestas debido al batching, desanidamos los elementos
        if "element" in df_raw.columns:
            df_bronze = df_raw.select(col("element.*"))
        else:
            df_bronze = df_raw

        # Añadir marcas de trazabilidad
        df_bronze = df_bronze.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("landing_source_file", element_at(split(input_file_name(), "/"), -1))

        # Escribir en la tabla Delta Bronze asegurando evolución de esquema
        df_bronze.write \
                 .format("delta") \
                 .mode("append") \
                 .option("mergeSchema", "true") \
                 .saveAsTable(bronze_table_name)

        logging.info(f"✅ Tabla Delta Bronze '{bronze_table_name}' actualizada correctamente.")