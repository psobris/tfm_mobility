import os
import logging
import requests
from datetime import datetime

#configuracion del registro de eventos para la auditoria del proceso
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class NASAIngester:
    """Clase encargada de descargar el historico semanal de incendios satelitales de NASA FIRMS"""

    def __init__(self, base_landing_path: str = "Files/landing/batch/nasa_historical"):
        #ruta base
        self.base_landing_path = base_landing_path.replace("/lakehouse/default/", "")
        #url oficial del conjunto de datos del sensor viirs para la zona europea
        self.firms_url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_7d.csv"

    def fetch_batch_to_landing(self) -> str:
        #ejecucion de la peticion http a la fuente externa con control de tiempo limite
        logging.info("[BATCH] Consultando histórico de incendios de NASA FIRMS")
        try:
            response = requests.get(self.firms_url, timeout=45)
            #verificacion de respuesta correcta antes de guardar
            if response.status_code != 200:
                logging.error(f"Error HTTP {response.status_code} en NASA FIRMS Batch")
                return None
            raw_data = response.text
        except Exception as e:
            logging.error(f"Excepcion en la ingesta NASA Batch: {e}")
            return None

        #genero rutas particionadas por fecha actual para organizar la capa landing
        now = datetime.now()
        dir_dest = os.path.join(
            self.base_landing_path,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d")
        )
        #construyo el directorio absoluto requerido para la persistencia en el sistema de archivos local
        local_dir = os.path.join("/lakehouse/default", dir_dest) if not dir_dest.startswith("/lakehouse/default") else dir_dest
        os.makedirs(local_dir, exist_ok=True)

        #se crea un nombre unico basado en timestamp para mantener la trazabilidad de las ingestas
        file_name = f"nasa_hist_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        local_file_path = os.path.join(local_dir, file_name)

        #se escribe el archivo csv sin modificaciones
        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(raw_data)

        #formateo de la ruta final limpia para ser retornada a los orquestadores de fabric
        spark_file_path = os.path.join(dir_dest, file_name).replace("\\", "/")
        logging.info(f"Histórico NASA guardado en: {spark_file_path}")
        return spark_file_path