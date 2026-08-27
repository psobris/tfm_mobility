import os
import json
import logging
from datetime import datetime, timezone
from tfm_mobility.utils.api_client import APIClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class WeatherIngester:
    """Ingestador para la API de Open-Meteo con guardado directo en OneLake de Microsoft Fabric."""

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, output_base_dir="/lakehouse/default/Files/landing/realtime/weather"):
        # Ruta absoluta hacia el montaje persistente de OneLake en Fabric
        self.output_base_dir = output_base_dir
        self.client = APIClient()

    def fetch_data(self, latitudes=[40.4168], longitudes=[-3.7038]) -> list:
        """
        Descarga la predicción meteorológica desde el presente hasta 16 días a futuro.
        Garantiza la extracción del dict/JSON nativo para evitar fallos de serialización.
        """
        results = []
        for lat, lon in zip(latitudes, longitudes):
            params = {
                "latitude": lat,
                "longitude": lon,
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
                response = self.client.get(self.BASE_URL, params=params)
                
                # Extraer siempre el contenido deserializable a JSON
                if hasattr(response, "json") and callable(response.json):
                    data = response.json()
                else:
                    data = response
                    
                results.append(data)
            except Exception as e:
                logging.error(f"❌ Error al consultar clima para lat={lat}, lon={lon}: {e}")
        return results

    def save_landing(self, data_list: list) -> str:
        """Guarda la respuesta JSON en OneLake particionada por fecha/hora actual."""
        now = datetime.now(timezone.utc)
        folder_path = os.path.join(
            self.output_base_dir,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d"),
            now.strftime("%H")
        )
        os.makedirs(folder_path, exist_ok=True)

        filename = f"weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
        file_path = os.path.join(folder_path, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_list, f, ensure_ascii=False, indent=2)

        logging.info(f"✅ [LANDING OK] Clima presente/futuro guardado en OneLake: {file_path}")
        return file_path

    def fetch_stream_to_landing(self) -> str:
        """Ejecuta la descarga y persistencia en un único paso."""
        data = self.fetch_data()
        return self.save_landing(data)


# ALIAS DE COMPATIBILIDAD CON NOMBRES DE CUADERNOS
WeatherIngestor = WeatherIngester