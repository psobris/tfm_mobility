import pytest
from unittest.mock import MagicMock, patch
from tfm_mobility.ingesters.realtime.weather_ingester import WeatherIngester

def test_weather_ingester_fetch_data():
    """Verifica que WeatherIngester consulte Open-Meteo para el presente y 16 días a futuro."""
    ingester = WeatherIngester()
    
    # Mockear la respuesta del cliente de API
    mock_response = {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "hourly": {
            "time": ["2026-08-12T00:00", "2026-08-12T01:00"],
            "temperature_2m": [25.0, 24.5]
        }
    }
    
    with patch.object(ingester.client, "get", return_value=mock_response) as mock_get:
        results = ingester.fetch_data(latitudes=[40.4168], longitudes=[-3.7038])
        
        # 1. Comprobar que se ha llamado a la API una vez
        assert mock_get.call_count == 1
        
        # 2. Verificar los parámetros pasados al cliente de API
        called_args, called_kwargs = mock_get.call_args
        params = called_kwargs.get("params", {})
        
        assert params["forecast_days"] == 16
        assert params["past_days"] == 0
        assert params["latitude"] == 40.4168
        assert params["longitude"] == -3.7038
        
        # 3. Comprobar el resultado devuelto
        assert len(results) == 1
        assert results[0] == mock_response

def test_weather_ingester_save_landing(tmp_path):
    """Verifica que los datos del clima se guarden correctamente en el directorio de Landing."""
    ingester = WeatherIngester(output_base_dir=str(tmp_path))
    sample_data = [{"latitude": 40.4168, "longitude": -3.7038}]
    
    saved_file = ingester.save_landing(sample_data)
    
    # Comprobar que el archivo se ha creado y no está vacío
    import os
    assert os.path.exists(saved_file)
    assert os.path.getsize(saved_file) > 0