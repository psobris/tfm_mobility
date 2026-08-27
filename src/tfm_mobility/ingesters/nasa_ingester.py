import os
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class NASAIngester:
    """Ingester Batch para datos históricos acumulados de incendios (NASA FIRMS)."""

    def __init__(self, base_landing_path: str = "Files/landing/batch/nasa_historical"):
        self.base_landing_path = base_landing_path.replace("/lakehouse/default/", "")
        self.firms_url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_7d.csv"

    def fetch_batch_to_landing(self) -> str:
        logging.info("📦 [BATCH] Consultando histórico de incendios de NASA FIRMS (7 días)...")
        try:
            response = requests.get(self.firms_url, timeout=45)
            if response.status_code != 200:
                logging.error(f"❌ Error HTTP {response.status_code} en NASA FIRMS Batch.")
                return None
            raw_data = response.text
        except Exception as e:
            logging.error(f"❌ Excepción en la ingesta NASA Batch: {e}")
            return None

        now = datetime.now()
        dir_dest = os.path.join(
            self.base_landing_path,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d")
        )
        local_dir = os.path.join("/lakehouse/default", dir_dest) if not dir_dest.startswith("/lakehouse/default") else dir_dest
        os.makedirs(local_dir, exist_ok=True)

        file_name = f"nasa_hist_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        local_file_path = os.path.join(local_dir, file_name)

        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(raw_data)

        spark_file_path = os.path.join(dir_dest, file_name).replace("\\", "/")
        logging.info(f"📥 [LANDING OK] Histórico NASA guardado en: {spark_file_path}")
        return spark_file_path