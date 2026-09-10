import os
import json
import pytest
from unittest.mock import MagicMock, patch
from tfm_mobility.ingesters.realtime.weather_ingester import WeatherIngester


def test_generate_spain_grid():
    #comprobamos la generacion de coordenadas espaciales garantizando los limites geográficos
    lats, lons = WeatherIngester.generate_spain_grid()
    assert len(lats) == len(lons)
    assert len(lats) > 2000
    assert min(lats) >= 27.0
    assert max(lats) <= 44.0


@patch("requests.Session.get")
def test_fetch_data_success(mock_get):
    #se valida la descarga exitosa simulando la respuesta de la api sobre un punto de prueba
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"latitude": 40.41, "longitude": -3.70, "hourly": {}}]
    mock_get.return_value = mock_response

    ingester = WeatherIngester()
    #reducimos la malla en el test para ejecucion instantanea
    with patch.object(WeatherIngester, "generate_spain_grid", return_value=([40.41], [-3.70])):
        data = ingester.fetch_data()

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["latitude"] == 40.41


def test_save_landing(tmp_path):
    #test para certificar la correcta escritura y estructura del json meteorologico en landing
    output_dir = str(tmp_path / "landing" / "weather")
    ingester = WeatherIngester(output_base_dir=output_dir)

    dummy_data = [{"latitude": 40.41, "longitude": -3.70, "hourly": {}}]
    saved_path = ingester.save_landing(dummy_data, timestamp_str="20260903_120000")

    assert os.path.exists(saved_path)
    with open(saved_path, "r", encoding="utf-8") as f:
        loaded_data = json.load(f)
    assert loaded_data == dummy_data


@patch("requests.Session.get")
def test_fetch_data_rate_limit_retry(mock_get):
    #comprobacion del reintento automatico cuando la api devuelve un codigo 429 por exceso de peticiones
    mock_429 = MagicMock()
    mock_429.status_code = 429
    mock_429.headers = {"Retry-After": "1"}

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = [{"latitude": 40.41, "longitude": -3.70, "hourly": {}}]

    mock_get.side_effect = [mock_429, mock_200]

    ingester = WeatherIngester()
    with patch.object(WeatherIngester, "generate_spain_grid", return_value=([40.41], [-3.70])), \
         patch("time.sleep", return_value=None):
        data = ingester.fetch_data()

    #se valida que la funcion maneja el rate limit reintentando la llamada hasta obtener exito
    assert len(data) == 1
    assert mock_get.call_count == 2