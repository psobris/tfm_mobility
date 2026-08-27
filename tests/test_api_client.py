import pytest
from unittest.mock import patch, MagicMock
from tfm_mobility.utils.api_client import APIClient

@patch("requests.get")
def test_api_client_real_request_success(mock_get):
    """Prueba una petición GET exitosa con mock."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"userId": 1, "id": 1}'
    mock_get.return_value = mock_response

    client = APIClient()
    response = client.get("https://fake-url.com/data")
    assert response is not None
    assert response.status_code == 200

@patch("requests.get")
def test_api_client_json_parsing(mock_get):
    """Prueba que el método get_json devuelve un diccionario correctamente."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 1, "title": "Test Title"}
    mock_get.return_value = mock_response

    client = APIClient()
    json_data = client.get_json("https://fake-url.com/data")
    assert json_data is not None
    assert json_data["id"] == 1
    assert json_data["title"] == "Test Title"