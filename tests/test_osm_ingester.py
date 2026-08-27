import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
from tfm_mobility.ingesters.osm_ingester import OSMIngester

@pytest.fixture
def mock_response_ok():
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {
        "elements": [
            {"id": 12345, "type": "way", "tags": {"highway": "motorway", "name": "A-6"}}
        ]
    }
    return mock

def test_fetch_roads_success(mock_response_ok):
    """Verifica que fetch_roads obtiene correctamente la estructura JSON de OSM."""
    ingester = OSMIngester()
    with patch("requests.post", return_value=mock_response_ok):
        data = ingester.fetch_roads()
        assert data is not None
        assert "elements" in data
        assert len(data["elements"]) == 1
        assert data["elements"][0]["id"] == 12345

def test_fetch_batch_to_landing_success(mock_response_ok):
    """Verifica que el JSON de OSM se guarda en la jerarquía particionada YYYY/MM/DD."""
    ingester = OSMIngester()
    
    with patch("requests.post", return_value=mock_response_ok), \
         patch("os.makedirs") as mock_makedirs, \
         patch("builtins.open", mock_open()):
        
        saved_path = ingester.fetch_batch_to_landing(landing_dir="Files/landing/batch/osm_roads")
        
        assert saved_path is not None
        assert "Files/landing/batch/osm_roads" in saved_path
        assert "osm_roads_" in saved_path
        assert saved_path.endswith(".json")
        mock_makedirs.assert_called_once()