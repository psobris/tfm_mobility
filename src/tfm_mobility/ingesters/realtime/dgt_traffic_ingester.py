import os
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DGTTrafficIngester:
    """Ingester NRT oficial para incidencias de tráfico en tiempo real (DGT DATEX II v3.7)."""

    def __init__(self, base_landing_path: str = "Files/landing/realtime/dgt_traffic"):
        self.base_landing_path = base_landing_path.replace("/lakehouse/default/", "")
        self.dgt_url = "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v37.xml"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TFM_Mobility_App/1.0"
        }

    def fetch_stream_to_landing(self) -> str:
        logging.info("⚡ [REAL-TIME] Consultando feed oficial DGT DATEX II v3.7...")
        try:
            response = requests.get(self.dgt_url, headers=self.headers, timeout=35)
            if response.status_code != 200 or len(response.content) < 100:
                logging.error(f"❌ Error HTTP {response.status_code} al consultar la DGT.")
                return None
            raw_data = response.text
        except Exception as e:
            logging.error(f"❌ Excepción en la ingesta de la DGT: {e}")
            return None

        now = datetime.now()
        dir_dest = os.path.join(
            self.base_landing_path,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d"),
            now.strftime("%H")
        )
        local_dir = os.path.join("/lakehouse/default", dir_dest) if not dir_dest.startswith("/lakehouse/default") else dir_dest
        os.makedirs(local_dir, exist_ok=True)

        file_name = f"dgt_incidents_{now.strftime('%Y%m%d_%H%M%S')}.xml"
        local_file_path = os.path.join(local_dir, file_name)

        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(raw_data)

        spark_file_path = os.path.join(dir_dest, file_name).replace("\\", "/")
        logging.info(f"📥 [LANDING OK] XML DATEX II guardado en: {spark_file_path}")
        return spark_file_path