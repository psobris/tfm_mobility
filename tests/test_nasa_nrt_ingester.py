from unittest.mock import patch
from tfm_mobility.ingesters.realtime.nasa_nrt_ingester import NASANRTIngester

def test_nasa_nrt_ingester_landing_success(tmp_path, requests_mock):
    #pruebo la descarga en tiempo real interceptando la llamada a la api de nasa firms
    fake_csv = "latitude,longitude,bright_ti4,confidence\n40.50,-3.80,340.2,h"
    
    #interceptamos cualquier llamada a la api de la nasa firms
    requests_mock.get(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv",
        text=fake_csv,
        status_code=200
    )

    ingester = NASANRTIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    #se valida que la funcion devuelva la ruta de destino nrt esperada
    assert result_path is not None
    assert "nasa_nrt" in result_path

def test_nasa_nrt_ingester_http_error(tmp_path, requests_mock):
    #test para comprobar la gestion de errores ante una falla http en el feed nrt
    requests_mock.get(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv",
        text="Server Error",
        status_code=500
    )

    ingester = NASANRTIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    #se comprueba que ante un error del servidor la funcion retorna none para evitar persistir datos corruptos
    assert result_path is None