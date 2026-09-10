import os
from unittest.mock import patch, MagicMock
from tfm_mobility.ingesters.realtime.dgt_traffic_ingester import DGTTrafficIngester

@patch("requests.get")
def test_dgt_traffic_ingester_success(mock_get, tmp_path):
    #pruebo que el ingester de la dgt procesa y guarda correctamente el xml cuando la respuesta es exitosa
    fake_xml = "<datex2><payloadPublication><incident><incidentTitle>Retencion A-6</incidentTitle></incident></payloadPublication></datex2>"
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = fake_xml
    mock_response.content = fake_xml.encode("utf-8")
    mock_get.return_value = mock_response

    ingester = DGTTrafficIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    #se valida que la ruta devuelvael valor esperado y contenga el prefijo de archivo asignado
    assert result_path is not None
    assert "dgt_incidents_" in result_path

@patch("requests.get")
def test_dgt_traffic_ingester_http_error(mock_get, tmp_path):
    #test para comprobar que ante un error del servidor http el ingester devuelva none de forma segura
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.content = b""
    mock_get.return_value = mock_response

    ingester = DGTTrafficIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    #verificacion del control de errores para no escribir archivos corruptos en la capa landing
    assert result_path is None