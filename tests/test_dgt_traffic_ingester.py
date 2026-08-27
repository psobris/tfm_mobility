import os
from unittest.mock import patch, MagicMock
from tfm_mobility.ingesters.realtime.dgt_traffic_ingester import DGTTrafficIngester

@patch("requests.get")
def test_dgt_traffic_ingester_success(mock_get, tmp_path):
    fake_xml = "<datex2><payloadPublication><incident><incidentTitle>Retención A-6</incidentTitle></incident></payloadPublication></datex2>"
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = fake_xml
    mock_response.content = fake_xml.encode("utf-8")
    mock_get.return_value = mock_response

    ingester = DGTTrafficIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_stream_to_landing()

    assert result_path is not None
    assert "dgt_incidents_" in result_path