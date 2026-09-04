import os
import json
import time
import logging
import requests
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class WeatherIngester:
    """
    Ingestador meteorológico para Open-Meteo API.
    Gestión de rate limiting por ventana temporal y reintentos ante HTTP 429/timeouts.
    """

    API_URL = "https://api.open-meteo.com/v1/forecast"
    BATCH_SIZE = 50
    MAX_COORDS_PER_MINUTE = 500
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 120
    MAX_RETRIES = 5

    def __init__(self, output_base_dir: str = "/lakehouse/default/Files/landing/realtime/weather"):
        self.output_base_dir = output_base_dir
        self.session = requests.Session()
        self.coords_in_current_window = 0
        self.window_start = time.time()

    @staticmethod
    def generate_spain_grid() -> Tuple[List[float], List[float]]:
        """Genera la malla nacional (~2.500 puntos terrestres)."""
        lats_p = np.linspace(36.0, 43.5, 40)
        lons_p = np.linspace(-9.0, 3.5, 55)

        lats, lons = [], []
        for lat in lats_p:
            for lon in lons_p:
                lats.append(round(float(lat), 2))
                lons.append(round(float(lon), 2))

        lats_c = np.linspace(27.6, 29.4, 10)
        lons_c = np.linspace(-18.1, -13.3, 30)

        for lat in lats_c:
            for lon in lons_c:
                lats.append(round(float(lat), 2))
                lons.append(round(float(lon), 2))

        return lats, lons

    def _control_rate_limit(self, num_coords: int) -> None:
        elapsed = time.time() - self.window_start
        if elapsed >= 60:
            self.coords_in_current_window = 0
            self.window_start = time.time()

        if self.coords_in_current_window + num_coords > self.MAX_COORDS_PER_MINUTE:
            remaining = 60 - (time.time() - self.window_start)
            if remaining > 0:
                logging.info(f"⏳ Límite de {self.MAX_COORDS_PER_MINUTE} coords/min alcanzado. Esperando {remaining:.1f}s...")
                time.sleep(remaining + 5)
            self.coords_in_current_window = 0
            self.window_start = time.time()

    def fetch_data(self) -> List[Dict[str, Any]]:
        lats, lons = self.generate_spain_grid()
        total_pts = len(lats)
        
        chunks = [
            (lats[i:i + self.BATCH_SIZE], lons[i:i + self.BATCH_SIZE])
            for i in range(0, total_pts, self.BATCH_SIZE)
        ]
        total_batches = len(chunks)
        results = []

        logging.info(f"🌐 Descargando predicción para {total_pts} puntos en {total_batches} lotes...")

        for batch_num, (batch_lats, batch_lons) in enumerate(chunks, start=1):
            num_coords = len(batch_lats)
            self._control_rate_limit(num_coords)

            params = {
                "latitude": ",".join(map(str, batch_lats)),
                "longitude": ",".join(map(str, batch_lons)),
                "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                "forecast_days": 7,
                "timezone": "Europe/Madrid"
            }

            conseguido = False
            for intento in range(1, self.MAX_RETRIES + 1):
                try:
                    res = self.session.get(
                        self.API_URL, 
                        params=params, 
                        timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT)
                    )

                    if res.status_code == 200:
                        data = res.json()
                        if not isinstance(data, list):
                            data = [data]
                        results.extend(data)
                        self.coords_in_current_window += num_coords
                        logging.info(f"  ✅ Lote {batch_num}/{total_batches} OK ({len(data)} puntos)")
                        conseguido = True
                        break

                    elif res.status_code == 429:
                        retry_after = res.headers.get("Retry-After")
                        espera = int(retry_after) + 5 if retry_after and retry_after.isdigit() else 65
                        logging.warning(f"  ⚠️ HTTP 429 - Rate limit alcanzado. Esperando {espera}s...")
                        time.sleep(espera)
                        self.coords_in_current_window = 0
                        self.window_start = time.time()

                    else:
                        logging.error(f"  ❌ HTTP {res.status_code}: {res.text[:200]}")
                        time.sleep(10 * intento)

                except Exception as e:
                    logging.warning(f"  ⚠️ Excepción en lote {batch_num} (intento {intento}): {e}")
                    time.sleep(10 * intento)

            if not conseguido:
                self.session.close()
                raise RuntimeError(f"❌ No se pudo descargar el lote {batch_num}/{total_batches}")

        self.session.close()
        return results

    def save_landing(self, data: List[Dict[str, Any]], timestamp_str: Optional[str] = None) -> str:
        if not timestamp_str:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        year, month, day, hour = timestamp_str[:4], timestamp_str[4:6], timestamp_str[6:8], timestamp_str[9:11]
        folder_path = os.path.join(self.output_base_dir, year, month, day, hour)
        os.makedirs(folder_path, exist_ok=True)

        full_path = os.path.join(folder_path, f"weather_{timestamp_str}.json")
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        logging.info(f"📁 JSON Meteorológico guardado en Landing: {full_path}")
        return full_path

    def run(self, timestamp_str: Optional[str] = None) -> str:
        data = self.fetch_data()
        return self.save_landing(data, timestamp_str)