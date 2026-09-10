import pytest
from unittest.mock import MagicMock, patch, mock_open
from tfm_mobility.processors.raw_processor import RawProcessor

@pytest.fixture
def mock_spark():
    #creo una sesion simulada de spark para ejecutar las pruebas del procesador raw
    return MagicMock()

def test_raw_processor_landing_to_raw_csv_realtime(mock_spark):
    #comprobacion de la conversion csv a parquet para los datos nrt procesados en tiempo real
    processor = RawProcessor(spark=mock_spark)
    
    mock_df = MagicMock()
    mock_df.count.return_value = 5
    mock_df.withColumn.return_value = mock_df
    
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.csv.return_value = mock_df
    mock_spark.read = mock_reader

    with patch("tfm_mobility.processors.raw_processor.F") as mock_f:
        mock_f.element_at.return_value = MagicMock()
        mock_f.split.return_value = MagicMock()
        mock_f.col.return_value = MagicMock()
        mock_f.current_timestamp.return_value = MagicMock()

        processor.landing_to_raw_csv("Files/landing/realtime/nasa_nrt", "Files/raw/realtime/nasa_nrt", is_batch=False)

        #se valida la adicion de columnas de metadatos y la escritura en formato parquet
        assert mock_df.withColumn.call_count == 2
        mock_df.write.format.assert_called_once_with("parquet")

def test_raw_processor_landing_to_raw_csv_batch(mock_spark):
    #test para verificar la conversion batch en modo sobrescritura
    processor = RawProcessor(spark=mock_spark)
    
    mock_df = MagicMock()
    mock_df.count.return_value = 100
    mock_df.withColumn.return_value = mock_df
    
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.csv.return_value = mock_df
    mock_spark.read = mock_reader

    with patch("tfm_mobility.processors.raw_processor.F") as mock_f:
        mock_f.element_at.return_value = MagicMock()
        mock_f.split.return_value = MagicMock()
        mock_f.col.return_value = MagicMock()
        mock_f.current_timestamp.return_value = MagicMock()

        processor.landing_to_raw_csv("Files/landing/batch/nasa_historical/nasa_hist.csv", "Files/raw/batch/nasa_historical", is_batch=True)

        #se confirma el uso del modo overwrite para los procesos masivos
        mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")

def test_raw_processor_landing_to_raw_osm(mock_spark):
    #pruebo el aplanamiento del json de osm manteniendo la estructura de la geometria
    processor = RawProcessor(spark=mock_spark)
    
    mock_df = MagicMock()
    mock_writer = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.write = mock_writer
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    mock_spark.createDataFrame.return_value = mock_df

    mock_osm_data = '{"elements": [{"id": 101, "type": "way", "tags": {"highway": "motorway", "name": "A-6"}, "geometry": [{"lat": 40.41, "lon": -3.70}]}]}'
    
    with patch("os.path.isdir", return_value=False), \
         patch("builtins.open", mock_open(read_data=mock_osm_data)), \
         patch("tfm_mobility.processors.raw_processor.F") as mock_f:
        
        mock_f.current_timestamp.return_value = MagicMock()

        processor.landing_to_raw_osm("Files/landing/batch/osm_roads/osm_roads.json", "Files/raw/batch/osm_roads")
        
        #validar que createdataframe recibio los registros estructurados con geometry_json
        assert mock_spark.createDataFrame.called
        records = mock_spark.createDataFrame.call_args[0][0]
        assert len(records) == 1
        assert "geometry_json" in records[0]
        mock_writer.format.assert_called_once_with("parquet")

def test_raw_processor_landing_to_raw_dgt_xml_no_files(mock_spark):
    #se comprueba la proteccion del procesador cuando no se localizan ficheros xml
    processor = RawProcessor(spark=mock_spark)
    with patch("os.path.exists", return_value=False):
        processor.landing_to_raw_dgt_xml("Files/landing/realtime/dgt_traffic", "Files/raw/realtime/dgt_traffic")
        mock_spark.createDataFrame.assert_not_called()

def test_raw_processor_landing_to_raw_json(mock_spark):
    #test para certificar la conversion de jsons estandar hacia parquet raw
    processor = RawProcessor(spark=mock_spark)

    mock_df = MagicMock()
    mock_df.count.return_value = 10
    mock_df.withColumn.return_value = mock_df

    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.json.return_value = mock_df
    mock_spark.read = mock_reader

    with patch("tfm_mobility.processors.raw_processor.F") as mock_f:
        mock_f.col.return_value = MagicMock()
        mock_f.lit.return_value = MagicMock()

        processor.landing_to_raw_json("Files/landing/realtime/weather", "Files/raw/realtime/weather", is_batch=False)

        #verificacion de la lectura json y escritura en parquet
        assert mock_reader.json.called
        mock_df.write.format.assert_called_once_with("parquet")