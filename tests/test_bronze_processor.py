import pytest
from unittest.mock import MagicMock, patch
from tfm_mobility.processors.bronze_processor import BronzeProcessor

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    return spark

def test_sanitize_column_names(mock_spark):
    """Verifica la limpieza de nombres de columnas incompatibles con Delta Lake."""
    processor = BronzeProcessor(spark=mock_spark)
    
    mock_df = MagicMock()
    mock_df.columns = ["{http://namespace}feedDescription", "tag_highway ", "id"]
    mock_df.withColumnRenamed.return_value = mock_df

    cleaned_df = processor._sanitize_column_names(mock_df)
    assert mock_df.withColumnRenamed.call_count == 3

def test_promote_batch_to_bronze(mock_spark):
    """Verifica la orquestación de la promoción Batch a capas Delta (NASA Histórico y OSM)."""
    processor = BronzeProcessor(spark=mock_spark)
    
    mock_df = MagicMock()
    mock_df.rdd.isEmpty.return_value = False
    mock_df.columns = ["id", "tag_highway", "landing_source_file", "ingestion_timestamp"]
    mock_df.orderBy.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df
    
    mock_spark.read.parquet.return_value = mock_df

    with patch.object(processor, "merge_into_bronze") as mock_merge:
        processor.promote_batch_to_bronze()
        
        # Se debe llamar dos veces: una para NASA Histórico y otra para OSM Roads
        assert mock_merge.call_count == 2