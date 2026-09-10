from unittest.mock import MagicMock, patch
import pytest
from tfm_mobility.processors.silver_processor import SilverProcessor


@pytest.fixture
def mock_spark():
    #fixture para simular la sesion de sparksession en las pruebas de la capa silver
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    return spark


def test_silver_processor_initialization(mock_spark):
    #compruebo que silverprocessor configura e inicializa correctamente la sesion
    processor = SilverProcessor(mock_spark)
    assert processor.spark == mock_spark


def test_process_silver_weather(mock_spark):
    #se valida el flujo de procesamiento desanidado de datos meteorologicos en silver
    processor = SilverProcessor(mock_spark)

    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df

    mock_spark.table.return_value = mock_df

    mock_column = MagicMock()
    mock_column.alias.return_value = mock_column
    mock_column.cast.return_value = mock_column
    mock_column.isNotNull.return_value = mock_column

    #configurar operadores de comparacion logica sobre el magicmock
    mock_column.__ge__.return_value = mock_column
    mock_column.__le__.return_value = mock_column
    mock_column.__and__.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.posexplode", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.to_timestamp", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.element_at", return_value=mock_column), \
         patch.object(processor, "_save_to_silver") as mock_save:

        processor.process_silver_weather()

        mock_spark.table.assert_called_with("bronze_weather")
        assert mock_save.call_count == 1


def test_process_silver_osm_roads_includes_geometry(mock_spark):
    #comprobacion de que la transformacion de carreteras extrae geometrias y centroides
    processor = SilverProcessor(mock_spark)

    mock_df = MagicMock()
    mock_df.columns = [
        "id", "nodes_count", "tag_highway", "tag_ref",
        "tag_name", "landing_source_file", "ingestion_timestamp",
        "geometry_json"
    ]
    mock_spark.table.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df

    mock_column = MagicMock()
    mock_column.alias.return_value = mock_column
    mock_column.cast.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.get_json_object", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.lit", return_value=mock_column), \
         patch.object(processor, "_save_to_silver") as mock_save:

        processor.process_silver_osm_roads()

        mock_spark.table.assert_any_call("bronze_osm_roads")
        assert mock_save.call_count == 1


def test_process_silver_dgt_traffic(mock_spark):
    #testpara verificar la promocion de incidencias de trafico dgt a silver
    processor = SilverProcessor(mock_spark)

    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.agg.return_value.collect.return_value = [{"latest_ingestion": None}]
    mock_spark.table.return_value = mock_df

    mock_column = MagicMock()
    mock_column.alias.return_value = mock_column
    mock_column.cast.return_value = mock_column
    mock_column.isNotNull.return_value = mock_column

    #simulacion de operadores logicos
    mock_column.__ge__.return_value = mock_column
    mock_column.__le__.return_value = mock_column
    mock_column.__and__.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.to_timestamp", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.coalesce", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.lit", return_value=mock_column), \
         patch("tfm_mobility.processors.silver_processor.F.max", return_value=mock_column), \
         patch.object(processor, "_save_to_silver") as mock_save:

        processor.process_silver_dgt_traffic()

        mock_spark.table.assert_called_with("bronze_dgt_traffic")
        assert mock_save.call_count == 1