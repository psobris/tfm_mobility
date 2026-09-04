import os
import json
import pytest
from unittest.mock import MagicMock, patch
from tfm_mobility.ingesters.realtime.weather_ingester import WeatherIngester


def test_generate_spain_grid():
    """Valida la generación de coordenadas sin llamadas de red."""
    lats, lons = WeatherIngester.generate_spain_grid()
    assert len(lats) == len(lons)
    assert len(lats) > 2000
    assert min(lats) >= 27.0
    assert max(lats) <= 44.0


@patch("requests.Session.get")
def test_fetch_data_success(mock_get):
    """Simula respuestas de la API para evitar bloquear el test runner."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"latitude": 40.41, "longitude": -3.70, "hourly": {}}]
    mock_get.return_value = mock_response

    ingester = WeatherIngester()
    # Reducimos la malla en el test para ejecución instantánea
    with patch.object(WeatherIngester, "generate_spain_grid", return_value=([40.41], [-3.70])):
        data = ingester.fetch_data()

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["latitude"] == 40.41


def test_save_landing(tmp_path):
    """Prueba la escritura en disco usando carpetas temporales aisladas."""
    output_dir = str(tmp_path / "landing" / "weather")
    ingester = WeatherIngester(output_base_dir=output_dir)

    dummy_data = [{"latitude": 40.41, "longitude": -3.70, "hourly": {}}]
    saved_path = ingester.save_landing(dummy_data, timestamp_str="20260903_120000")

    assert os.path.exists(saved_path)
    with open(saved_path, "r", encoding="utf-8") as f:
        loaded_data = json.load(f)
    assert loaded_data == dummy_data