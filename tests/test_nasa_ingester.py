import os
from unittest.mock import patch, MagicMock
from tfm_mobility.ingesters.nasa_ingester import NASAIngester

@patch("requests.get")
def test_nasa_batch_ingester_success(mock_get, tmp_path):
    fake_csv = "latitude,longitude,brightness\n40.41,-3.70,300.5"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = fake_csv
    mock_get.return_value = mock_response

    ingester = NASAIngester(base_landing_path=str(tmp_path))
    result_path = ingester.fetch_batch_to_landing()

    assert result_path is not None
    assert "nasa_hist_" in result_path