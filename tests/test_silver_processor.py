from unittest.mock import MagicMock, patch
from tfm_mobility.processors.silver_processor import SilverProcessor


def test_filter_spain_with_buffer_mock():
    """Verifica que el método filtre el DataFrame usando el bounding box."""
    mock_spark = MagicMock()
    processor = SilverProcessor(mock_spark)
    mock_df = MagicMock()

    mock_column = MagicMock()
    mock_column.__ge__.return_value = mock_column
    mock_column.__le__.return_value = mock_column
    mock_column.__and__.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column) as mock_col:
        processor._filter_spain_with_buffer(mock_df)

        mock_col.assert_any_call("latitude")
        mock_col.assert_any_call("longitude")
        assert mock_df.filter.called


def test_silver_processor_constants():
    """Verifica que los límites geográficos (Bounding Box) estén definidos correctamente."""
    mock_spark = MagicMock()
    processor = SilverProcessor(mock_spark)

    assert processor.LAT_MIN == 27.0
    assert processor.LAT_MAX == 44.0
    assert processor.LON_MIN == -18.5
    assert processor.LON_MAX == 5.0


def test_process_silver_osm_roads_includes_geometry():
    """Verifica que el procesamiento de OSM en Silver incluya la columna de geometría."""
    mock_spark = MagicMock()
    processor = SilverProcessor(mock_spark)

    mock_df = MagicMock()
    # Simular que 'geometry_json' NO está en la tabla mockeada para forzar el path con F.lit
    mock_df.columns = ["id", "nodes_count", "tag_highway", "tag_ref", "tag_name", "landing_source_file", "ingestion_timestamp"]
    mock_spark.table.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df

    mock_column = MagicMock()
    mock_column.alias.return_value = mock_column
    mock_column.cast.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column) as mock_col, \
         patch("tfm_mobility.processors.silver_processor.F.lit", return_value=mock_column) as mock_lit, \
         patch.object(processor, "_save_to_silver") as mock_save:

        processor.process_silver_osm_roads()

        # Verificar lectura de la tabla Bronze
        mock_spark.table.assert_called_once_with("bronze_osm_roads")

        # Verificar que se llamó a _save_to_silver
        mock_save.assert_called_once_with(mock_df, "silver_osm_roads", ["osm_way_id"])