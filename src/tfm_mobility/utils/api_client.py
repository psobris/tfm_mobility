import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class APIClient:
    """Cliente HTTP centralizado para peticiones a APIs externas."""

    def __init__(self, timeout: int = 90):
        self.timeout = timeout

    def get(self, url: str, params: dict = None, headers: dict = None):
        """Petición GET básica devolviendo el objeto Response."""
        try:
            response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            logging.error(f"❌ Error GET en {url}: {e}")
            return None

    def get_text(self, url: str, params: dict = None, headers: dict = None) -> str:
        """Petición GET que retorna directamente el texto (CSV, XML, HTML)."""
        res = self.get(url, params=params, headers=headers)
        return res.text if res else None

    def get_json(self, url: str, params: dict = None, headers: dict = None) -> dict:
        """Petición GET que retorna directamente un diccionario JSON estructurado."""
        res = self.get(url, params=params, headers=headers)
        return res.json() if res else None

    def post(self, url: str, data: dict = None, json: dict = None, headers: dict = None):
        """Petición POST (usada para la Overpass API de OpenStreetMap)."""
        try:
            response = requests.post(url, data=data, json=json, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            logging.error(f"❌ Error POST en {url}: {e}")
            return None