import os
import requests
from unittest.mock import MagicMock, patch
import pytest
from tfm_mobility.ingesters.osm_ingester import OSMIngester


@pytest.fixture
def osm_ingester(tmp_path):
    #fixture que proporciona una instancia de osmingester con ruta temporal
    return OSMIngester(landing_base_path=str(tmp_path))


@pytest.fixture
def mock_response_ok():
    #fixture que simula una respuesta http exitosa de overpass
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "version": 0.6,
        "generator": "Overpass API",
        "elements": [
            {
                "type": "way",
                "id": 123456,
                "nodes": [1, 2, 3],
                "tags": {"highway": "motorway", "ref": "A-1"},
            }
        ],
    }
    return mock_resp


def test_build_query_contains_expected_highways(osm_ingester):
    #compruebo que la consulta overpass incluye las categorias de carreteras requeridas
    query = osm_ingester._build_query()

    assert "motorway" in query
    assert "trunk" in query
    assert "motorway_link" in query
    assert "trunk_link" in query

    #compatibilidad con salida ordenada o sin ordenar
    assert "out qt body geom;" in query or "out body geom;" in query


def test_fetch_roads_sends_expected_request(
    osm_ingester,
    mock_response_ok,
):
    #se valida que la peticion a overpass utiliza post y contiene las cabeceras y configuraciones necesarias
    with patch(
        "requests.post",
        return_value=mock_response_ok,
    ) as mock_post:
        osm_ingester.fetch_roads()

    assert mock_post.called

    kwargs = mock_post.call_args.kwargs

    assert "data" in kwargs
    assert "headers" in kwargs
    assert "timeout" in kwargs

    assert kwargs["timeout"] == osm_ingester.REQUEST_TIMEOUT

    headers = kwargs["headers"]

    assert "User-Agent" in headers
    assert headers["User-Agent"] == osm_ingester.USER_AGENT

    query = kwargs["data"]["data"]

    assert "motorway" in query
    assert "trunk" in query
    assert "out qt body geom;" in query or "out body geom;" in query


def test_save_to_landing(osm_ingester):
    #test para comprobar la persistencia correcta del json descargado en la zona landing
    fake_data = {"elements": [{"id": 1, "type": "way"}]}
    output_path = osm_ingester.save_to_landing(fake_data)

    #verificacion de la existencia del archivo guardado en el sistema de ficheros
    assert output_path is not None
    assert os.path.exists(output_path)
    assert "osm_roads_" in output_path


def test_fetch_roads_exhaust_endpoints_raises_runtime_error(osm_ingester):
    #se verifica que ante la caida continuada de todos los endpoints el ingester lance un runtimeerror
    with patch("requests.post", side_effect=requests.RequestException("Error de conexion")), \
         patch("time.sleep", return_value=None):
        with pytest.raises(RuntimeError):
            osm_ingester.fetch_roads()