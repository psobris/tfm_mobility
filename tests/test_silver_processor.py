from unittest.mock import MagicMock, patch
from tfm_mobility.processors.silver_processor import SilverProcessor


def test_filter_spain_with_buffer_mock():
    """Verifica que el método filtre el DataFrame usando el bounding box."""
    mock_spark = MagicMock()
    processor = SilverProcessor(mock_spark)
    mock_df = MagicMock()

    # Mockeamos F.col para que devuelva un objeto con soporte para operadores de comparación de PySpark
    mock_column = MagicMock()
    mock_column.__ge__.return_value = mock_column
    mock_column.__le__.return_value = mock_column
    mock_column.__and__.return_value = mock_column

    with patch("tfm_mobility.processors.silver_processor.F.col", return_value=mock_column) as mock_col:
        processor._filter_spain_with_buffer(mock_df)

        # Verificamos que se invocó F.col con latitude y longitude
        mock_col.assert_any_call("latitude")
        mock_col.assert_any_call("longitude")
        # Verificamos que el DataFrame ejecutó su método .filter()
        assert mock_df.filter.called


def test_silver_processor_constants():
    """Verifica que los límites geográficos (Bounding Box) estén definidos correctamente."""
    mock_spark = MagicMock()
    processor = SilverProcessor(mock_spark)

    assert processor.LAT_MIN == 27.0
    assert processor.LAT_MAX == 44.0
    assert processor.LON_MIN == -18.5
    assert processor.LON_MAX == 5.0