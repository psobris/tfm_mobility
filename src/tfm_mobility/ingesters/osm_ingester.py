import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class OSMIngester:
    """
    Ingester para OpenStreetMap mediante la API Overpass optimizado.
    """

    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://z.overpass-api.de/api/interpreter",
        "https://overpass.private.coffee/api/interpreter",
    ]

    USER_AGENT = "TFM-Emergency-Mobility/1.0 (Microsoft Fabric academic project)"

    # España peninsular + Baleares + Canarias
    SOUTH = 27.0
    WEST = -18.5
    NORTH = 44.0
    EAST = 5.0

    REQUEST_TIMEOUT = 300
    MAX_RETRIES_PER_ENDPOINT = 2

    def __init__(self, landing_base_path: str):
        self.landing_base_path = landing_base_path

    def _build_query(self) -> str:
        """
        Query optimizada de Overpass con salida sin ordenar (qt) para prevenir Timeout 504.
        """
        return f"""
        [out:json][timeout:180][maxsize:1073741824];
        (
          way["highway"="motorway"]({self.SOUTH},{self.WEST},{self.NORTH},{self.EAST});
          way["highway"="trunk"]({self.SOUTH},{self.WEST},{self.NORTH},{self.EAST});
          way["highway"="motorway_link"]({self.SOUTH},{self.WEST},{self.NORTH},{self.EAST});
          way["highway"="trunk_link"]({self.SOUTH},{self.WEST},{self.NORTH},{self.EAST});
        );
        out qt body geom;
        """

    def fetch_roads(self) -> dict:
        query = self._build_query()
        headers = {
            "User-Agent": self.USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        last_error: Optional[Exception] = None

        for endpoint in self.OVERPASS_ENDPOINTS:
            for attempt in range(1, self.MAX_RETRIES_PER_ENDPOINT + 1):
                try:
                    logging.info(f"Consultando Overpass: {endpoint} (intento {attempt})")
                    response = requests.post(
                        endpoint,
                        data={"data": query},
                        headers=headers,
                        timeout=self.REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()

                    data = response.json()
                    if not isinstance(data, dict):
                        raise ValueError("La respuesta de Overpass no tiene formato JSON esperado.")

                    elements = data.get("elements", [])
                    logging.info(f"✅ Respuesta OSM recibida correctamente: {len(elements)} elementos.")
                    return data

                except (requests.RequestException, ValueError) as exc:
                    last_error = exc
                    logging.warning(f"⚠️ Error en Overpass (endpoint={endpoint}, intento={attempt}): {exc}")

                if attempt < self.MAX_RETRIES_PER_ENDPOINT:
                    wait_seconds = 10 * attempt
                    logging.info(f"Esperando {wait_seconds}s antes de reintentar...")
                    time.sleep(wait_seconds)

            logging.warning(f"Endpoint Overpass agotado: {endpoint}")

        raise RuntimeError(f"No ha sido posible obtener datos de OSM. Último error: {last_error}")

    def save_to_landing(
        self,
        data: dict,
        ingestion_timestamp: Optional[datetime] = None,
    ) -> str:

        if ingestion_timestamp is None:
            ingestion_timestamp = datetime.now(timezone.utc)

        timestamp_str = ingestion_timestamp.strftime("%Y%m%d_%H%M%S")

        landing_dir = (
            Path(self.landing_base_path)
            / "landing"
            / "batch"
            / "osm_roads"
            / ingestion_timestamp.strftime("%Y")
            / ingestion_timestamp.strftime("%m")
            / ingestion_timestamp.strftime("%d")
        )

        landing_dir.mkdir(parents=True, exist_ok=True)
        output_path = landing_dir / f"osm_roads_{timestamp_str}.json"

        import json
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False)

        logging.info(f"Datos OSM guardados en Landing: {output_path}")
        return str(output_path)

    def fetch_batch_to_landing(self) -> str:
        logging.info("Iniciando ingesta OSM...")
        data = self.fetch_roads()
        path = self.save_to_landing(data)
        logging.info(f"Ingesta OSM finalizada: {path}")
        return path