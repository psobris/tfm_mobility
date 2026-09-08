from unittest.mock import MagicMock, patch
import pytest
from tfm_mobility.processors.bronze_processor import BronzeProcessor


@pytest.fixture
def mock_spark():
    """Fixture para simular la sesión de SparkSession."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    return spark


def test_promote_realtime_to_bronze(mock_spark):
    """Verifica la orquestación de las fuentes en tiempo real a capas Delta."""
    processor = BronzeProcessor(spark=mock_spark)

    mock_df = MagicMock()
    mock_df.take.return_value = [True]

    with patch.object(processor, "process_bronze_weather") as mock_weather, \
         patch.object(processor, "_read_raw_data", return_value=mock_df), \
         patch.object(processor, "merge_into_bronze") as mock_merge:

        processor.promote_realtime_to_bronze()

        # Verifica que se llama a process_bronze_weather y a merge_into_bronze para dgt_traffic y nasa_nrt
        assert mock_weather.call_count == 1
        assert mock_merge.call_count == 2


def test_promote_batch_to_bronze(mock_spark):
    """Verifica la orquestación de la promoción Batch a capas Delta (NASA Histórico, OSM Roads y OSM Places)."""
    processor = BronzeProcessor(spark=mock_spark)

    mock_df = MagicMock()
    mock_df.take.return_value = [True]
    mock_df.columns = ["id", "tag_highway", "landing_source_file", "ingestion_timestamp"]

    with patch.object(processor, "_read_raw_data", return_value=mock_df), \
         patch.object(processor, "merge_into_bronze") as mock_merge:

        processor.promote_batch_to_bronze()

        # NASA Historical, OSM Roads y OSM Places
        assert mock_merge.call_count == 3