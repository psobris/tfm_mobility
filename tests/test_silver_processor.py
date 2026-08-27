import pytest
from unittest.mock import MagicMock, patch
from tfm_mobility.processors.silver_processor import SilverProcessor

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    return spark

def test_process_silver_weather(mock_spark):
    processor = SilverProcessor(spark=mock_spark)
    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_spark.table.return_value = mock_df

    with patch.object(processor, "_save_to_silver") as mock_save, \
         patch("tfm_mobility.processors.silver_processor.F") as mock_f:
        mock_f.col.return_value = MagicMock()
        mock_f.posexplode.return_value = MagicMock()
        mock_f.to_timestamp.return_value = MagicMock()
        mock_f.element_at.return_value = MagicMock()

        processor.process_silver_weather()
        mock_save.assert_called_once()
        assert mock_save.call_args[0][1] == "silver_weather"

def test_process_silver_nasa_fires(mock_spark):
    processor = SilverProcessor(spark=mock_spark)
    mock_df = MagicMock()
    mock_df.unionByName.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_spark.table.return_value = mock_df

    with patch.object(processor, "_save_to_silver") as mock_save, \
         patch("tfm_mobility.processors.silver_processor.F") as mock_f:
        mock_f.col.return_value = MagicMock()
        mock_f.lpad.return_value = MagicMock()
        mock_f.concat_ws.return_value = MagicMock()
        mock_f.to_timestamp.return_value = MagicMock()

        processor.process_silver_nasa_fires()
        mock_save.assert_called_once()
        assert mock_save.call_args[0][1] == "silver_nasa_fires"

def test_process_silver_dgt_traffic(mock_spark):
    processor = SilverProcessor(spark=mock_spark)
    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_spark.table.return_value = mock_df

    with patch.object(processor, "_save_to_silver") as mock_save, \
         patch("tfm_mobility.processors.silver_processor.F") as mock_f:
        mock_f.col.return_value = MagicMock()
        mock_f.coalesce.return_value = MagicMock()
        mock_f.lit.return_value = MagicMock()
        mock_f.to_timestamp.return_value = MagicMock()

        processor.process_silver_dgt_traffic()
        mock_save.assert_called_once()
        assert mock_save.call_args[0][1] == "silver_dgt_traffic"

def test_process_silver_osm_roads(mock_spark):
    processor = SilverProcessor(spark=mock_spark)
    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_spark.table.return_value = mock_df

    with patch.object(processor, "_save_to_silver") as mock_save, \
         patch("tfm_mobility.processors.silver_processor.F") as mock_f:
        mock_f.col.return_value = MagicMock()

        processor.process_silver_osm_roads()
        mock_save.assert_called_once()
        assert mock_save.call_args[0][1] == "silver_osm_roads"