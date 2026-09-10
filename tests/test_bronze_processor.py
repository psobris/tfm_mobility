from unittest.mock import MagicMock, patch
import pytest
from tfm_mobility.processors.bronze_processor import BronzeProcessor


@pytest.fixture
def mock_spark():
    #aqui creo una sesion simulada de spark para ejecutar las pruebas sin depender de un cluster real
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    return spark


def test_promote_realtime_to_bronze(mock_spark):
    #comprobacion de la orquestacion de fuentes en tiempo real verificando las llamadas a las funciones de promocion
    processor = BronzeProcessor(spark=mock_spark)

    mock_df = MagicMock()
    mock_df.take.return_value = [True]

    with patch.object(processor, "process_bronze_weather") as mock_weather, \
         patch.object(processor, "_read_raw_data", return_value=mock_df), \
         patch.object(processor, "merge_into_bronze") as mock_merge:

        processor.promote_realtime_to_bronze()

        #se valida la ejecucion de la promocion meteorologica y los dos mergings de trafico y nasa nrt
        assert mock_weather.call_count == 1
        assert mock_merge.call_count == 2


def test_promote_batch_to_bronze(mock_spark):
    #test para validar el flujo batch verificando la promocion del historico nasa carreteras y lugares
    processor = BronzeProcessor(spark=mock_spark)

    mock_df = MagicMock()
    mock_df.take.return_value = [True]
    mock_df.columns = ["id", "tag_highway", "landing_source_file", "ingestion_timestamp"]

    with patch.object(processor, "_read_raw_data", return_value=mock_df), \
         patch.object(processor, "merge_into_bronze") as mock_merge:

        processor.promote_batch_to_bronze()

        #verificacion de las tres invocaciones requeridas para las fuentes estaticas
        assert mock_merge.call_count == 3


def test_resolve_fabric_path(mock_spark):
    #verifico que la funcion auxiliar limpia y estandariza correctamente las rutas del sistema de archivos
    processor = BronzeProcessor(spark=mock_spark)
    
    path_input = "/lakehouse/default/Files/raw/realtime/weather"
    resolved_path = processor._resolve_fabric_path(path_input)
    
    assert resolved_path == "Files/raw/realtime/weather"


def test_merge_into_bronze_empty_dataframe(mock_spark):
    #se comprueba que ante un dataframe vacio el procesador detenga la ejecucion sin lanzar errores
    processor = BronzeProcessor(spark=mock_spark)
    
    empty_df = MagicMock()
    empty_df.take.return_value = []
    
    with patch.object(processor, "_sanitize_column_names") as mock_sanitize:
        processor.merge_into_bronze(empty_df, "test_table", ["id"])
        assert mock_sanitize.call_count == 0