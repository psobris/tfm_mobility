from unittest.mock import patch
from tfm_mobility.ingesters.realtime.nasa_nrt_ingester import NASANRTIngester

def test_nasa_nrt_ingester_landing_success(tmp_path, requests_mock):
    fake_csv = "latitude,longitude,bright_ti4,confidence\n40.50,-3.80,340.2,h"
    
    # Interceptamos cualquier llamada a la API de la NASA FIRMS
    requests_mock.get(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv",
        text=fake_csv,
        status_code=200
    )

    ingester = NASANRTIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    assert result_path is not None
    assert "nasa_nrt" in result_path