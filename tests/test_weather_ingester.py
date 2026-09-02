import os
import json
from unittest.mock import patch, MagicMock
from tfm_mobility.ingesters.realtime.weather_ingester import WeatherIngester


def test_weather_ingester_grid_generation():
    """Verifica que el generador de la malla a 0.1° retorne listas válidas."""
    lats, lons = WeatherIngester.generate_spain_grid(step=0.1)

    assert len(lats) == len(lons)
    assert len(lats) > 10000  # A 0.1° deben generarse mas de 10.000 puntos
    assert 40.41 in lats or 40.4 in lats  # Coordenadas aproximadas de Madrid
    assert -3.7 in lons or -3.71 in lons


def test_weather_ingester_fetch_data_mock(requests_mock):
    """Verifica que fetch_data envíe los parámetros HTTP en lotes correctamente."""
    ingester = WeatherIngester(output_base_dir="/tmp/test_weather")

    mock_response = {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "hourly": {
            "time": ["2026-09-02T00:00"],
            "temperature_2m": [22.5]
        }
    }

    requests_mock.get(WeatherIngester.BASE_URL, json=mock_response)

    results = ingester.fetch_data(latitudes=[40.4168], longitudes=[-3.7038])

    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0]["latitude"] == 40.4168

    last_request = requests_mock.last_request
    assert "latitude" in last_request.query
    assert "longitude" in last_request.query
    assert "timezone=europe" in last_request.query.lower()


def test_weather_ingester_save_landing(tmp_path):
    """Verifica que la función guarde los archivos en la estructura de directorios adecuada."""
    test_dir = str(tmp_path)
    ingester = WeatherIngester(output_base_dir=test_dir)

    dummy_data = [{"latitude": 40.0, "longitude": -3.0, "hourly": {}}]
    timestamp_str = "20260902_120000"

    saved_file = ingester.save_landing(dummy_data, timestamp_str)

    assert os.path.exists(saved_file)
    with open(saved_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert len(data) == 1
        assert data[0]["latitude"] == 40.0