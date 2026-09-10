import pytest
import requests
from unittest.mock import patch, MagicMock
from tfm_mobility.utils.api_client import APIClient

@patch("requests.get")
def test_api_client_real_request_success(mock_get):
    #pruebo que el cliente maneja correctamente las peticiones get exitosas
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
    #comprobacion del parseo json cuando el endpoint responde con un diccionario valido
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 1, "title": "Test Title"}
    mock_get.return_value = mock_response

    client = APIClient()
    json_data = client.get_json("https://fake-url.com/data")
    assert json_data is not None
    assert json_data["id"] == 1
    assert json_data["title"] == "Test Title"

@patch("requests.get")
def test_api_client_http_error_handling(mock_get):
    #verifica la respuesta del cliente ante errores 404 o 500
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    client = APIClient()
    response = client.get("https://fake-url.com/not-found")
    assert response is not None
    assert response.status_code == 404

@patch("requests.get")
def test_api_client_timeout_exception(mock_get):
    #se verifica que ante un timeout de red el cliente capture la excepcion y retorne none de forma segura
    mock_get.side_effect = requests.exceptions.Timeout("Timeout de conexion")

    client = APIClient()
    response = client.get("https://fake-url.com/timeout")
    assert response is None