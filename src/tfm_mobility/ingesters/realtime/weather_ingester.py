import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from tfm_mobility.utils.api_client import APIClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class WeatherIngester:
    """
    Ingestador meteorológico para Open-Meteo API.
    Genera una malla ultra fina de cobertura nacional (~10km) dividida en lotes (batching)
    para evitar límites de la API HTTP y guardarla en la capa Landing (JSON).
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, output_base_dir: str = "/lakehouse/default/Files/landing/realtime/weather"):
        """
        Inicializa el ingestador con la ruta de salida en OneLake / Lakehouse.
        """
        self.output_base_dir = output_base_dir
        self.client = APIClient()

    @staticmethod
    def generate_spain_grid(step: float = 0.1) -> Tuple[List[float], List[float]]:
        """
        Genera una malla regular de coordenadas con resolución ~10km (step=0.1) 
        que cubre el 100% del territorio español (Península, Baleares y Canarias).
        """
        lats, lons = [], []

        # 1. Península e Islas Baleares (Lat: 35.8 a 43.8, Lon: -9.5 a 4.5)
        lat_p = 35.8
        while lat_p <= 43.8:
            lon_p = -9.5
            while lon_p <= 4.5:
                lats.append(round(lat_p, 2))
                lons.append(round(lon_p, 2))
                lon_p += step
            lat_p += step

        # 2. Islas Canarias (Lat: 27.5 a 29.5, Lon: -18.2 a -13.2)
        lat_c = 27.5
        while lat_c <= 29.5:
            lon_c = -18.2
            while lon_c <= -13.2:
                lats.append(round(lat_c, 2))
                lons.append(round(lon_c, 2))
                lon_c += step
            lat_c += step

        return lats, lons

    def fetch_data(self, latitudes: Optional[List[float]] = None, longitudes: Optional[List[float]] = None) -> List[Dict[str, Any]]:
        """
        Descarga la predicción meteorológica dividiendo las ~12.000 coordenadas en lotes
        de 400 puntos para garantizar la compatibilidad con el servidor API.
        """
        if latitudes is None or longitudes is None:
            latitudes, longitudes = self.generate_spain_grid(step=0.1)

        batch_size = 400
        results = []

        logging.info(f"🌐 Iniciando descarga meteorológica para {len(latitudes)} puntos a resolución ~10km...")

        for i in range(0, len(latitudes), batch_size):
            batch_lats = latitudes[i:i + batch_size]
            batch_lons = longitudes[i:i + batch_size]

            params = {
                "latitude": ",".join(map(str, batch_lats)),
                "longitude": ",".join(map(str, batch_lons)),
                "hourly": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "rain",
                    "showers",
                    "snowfall",
                    "wind_speed_10m",
                    "wind_gusts_10m",
                    "precipitation_probability"
                ],
                "forecast_days": 16,
                "past_days": 0,
                "timezone": "Europe/Madrid"
            }

            try:
                logging.info(f" └─ Procesando lote {i // batch_size + 1}/{(len(latitudes) + batch_size - 1) // batch_size} ({len(batch_lats)} puntos)...")
                response = self.client.get(self.BASE_URL, params=params)
                
                data = response.json() if hasattr(response, "json") and callable(response.json) else response

                if isinstance(data, list):
                    results.extend(data)
                elif isinstance(data, dict):
                    results.append(data)

            except Exception as e:
                logging.error(f"❌ Error al consultar el lote {i}: {e}")

        logging.info(f"✅ Descarga completada. Se obtuvieron {len(results)} respuestas de nodos meteorológicos.")
        return results

    def save_landing(self, data: List[Dict[str, Any]], timestamp_str: Optional[str] = None) -> str:
        """
        Guarda las respuestas acumuladas en un único archivo JSON en la capa Landing.
        Soporta ser invocado con o sin timestamp_str.
        """
        if not timestamp_str:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        year = timestamp_str[:4]
        month = timestamp_str[4:6]
        day = timestamp_str[6:8]
        hour = timestamp_str[9:11]

        folder_path = os.path.join(self.output_base_dir, year, month, day, hour)
        os.makedirs(folder_path, exist_ok=True)

        file_name = f"weather_{timestamp_str}.json"
        full_path = os.path.join(folder_path, file_name)

        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logging.info(f"📁 Datos meteorológicos guardados exitosamente en: {full_path}")
        return full_path

    def run(self, timestamp_str: Optional[str] = None) -> str:
        """
        Flujo de ejecución principal del ingestador.
        """
        if not timestamp_str:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        data = self.fetch_data()
        saved_path = self.save_landing(data, timestamp_str)
        return saved_path