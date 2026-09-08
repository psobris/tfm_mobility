from unittest.mock import MagicMock, patch
import pytest
from tfm_mobility.ingesters.osm_ingester import OSMIngester


@pytest.fixture
def osm_ingester(tmp_path):
    """Fixture que proporciona una instancia de OSMIngester con ruta temporal."""
    return OSMIngester(landing_base_path=str(tmp_path))


@pytest.fixture
def mock_response_ok():
    """Fixture que simula una respuesta HTTP exitosa de Overpass."""
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
    """
    Verifica que la consulta Overpass incluye las principales
    categorías de carreteras utilizadas por el proyecto.
    """
    query = osm_ingester._build_query()

    assert "motorway" in query
    assert "trunk" in query
    assert "motorway_link" in query
    assert "trunk_link" in query

    # Compatibilidad con salida ordenada o sin ordenar (qt)
    assert "out qt body geom;" in query or "out body geom;" in query


def test_fetch_roads_sends_expected_request(
    osm_ingester,
    mock_response_ok,
):
    """
    Verifica que la petición a Overpass utiliza POST y contiene
    la query esperada y User-Agent.
    """
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