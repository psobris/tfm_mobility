import logging
import os
import json
import requests
from datetime import datetime
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter"
]

class OSMIngester:
    """Ingestor para la red viaria de España desde OpenStreetMap (Overpass API)."""

    def __init__(self, bbox: str = "35.8,-9.3,43.8,3.3", **kwargs):
        self.bbox = bbox

    def fetch_roads(self) -> Optional[Dict[str, Any]]:
        # Query optimizada para carreteras principales de España
        query = f"""
        [out:json][timeout:60];
        (
          way["highway"~"motorway|trunk"]({self.bbox});
        );
        out body qt 5000;
        """
        
        headers = {
            "User-Agent": "TFMMobilityApp/1.0 (contact: pasobrad@ucm.es)",
            "Accept": "application/json"
        }

        for endpoint in OVERPASS_ENDPOINTS:
            try:
                logging.info(f"🗺️ [OSM] Solicitando carreteras a Overpass: {endpoint}...")
                response = requests.post(endpoint, data={"data": query}, headers=headers, timeout=65)
                
                if response.status_code == 200:
                    data = response.json()
                    logging.info(f"✅ [OSM OK] Obtenidos {len(data.get('elements', []))} tramos de carretera.")
                    return data
                else:
                    logging.warning(f"⚠️ [OSM WARNING] Servidor {endpoint} devolvió código: {response.status_code}")
            except Exception as e:
                logging.warning(f"⚠️ [OSM WARNING] Falló endpoint {endpoint}: {e}")
                continue

        logging.error("❌ [OSM ERROR] Fallaron todos los endpoints de Overpass.")
        return None

    def fetch_stream_to_landing(self, landing_dir: str = "Files/landing/batch/osm_roads") -> Optional[str]:
        """Guarda la respuesta JSON de OSM en la zona Landing Batch organizada por YYYY/MM/DD."""
        data = self.fetch_roads()
        if not data:
            return None

        now = datetime.now()
        clean_landing = landing_dir.replace("/lakehouse/default/", "")
        
        # Estructurar la ruta particionada por Fecha (YYYY/MM/DD)
        partitioned_path = os.path.join(
            clean_landing,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d")
        )
        
        local_dir = os.path.join("/lakehouse/default", partitioned_path) if not partitioned_path.startswith("/lakehouse/default") else partitioned_path
        os.makedirs(local_dir, exist_ok=True)

        file_name = f"osm_roads_{now.strftime('%Y%m%d_%H%M%S')}.json"
        local_file_path = os.path.join(local_dir, file_name)

        with open(local_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        spark_file_path = os.path.join(partitioned_path, file_name).replace("\\", "/")
        logging.info(f"💾 [OSM LANDING] Red viaria guardada en: {spark_file_path}")
        return spark_file_path

    def fetch_batch_to_landing(self, landing_dir: str = "Files/landing/batch/osm_roads") -> Optional[str]:
        """Alias de compatibilidad para cuadernos batch."""
        return self.fetch_stream_to_landing(landing_dir=landing_dir)