import requests
import json

# Cabecera para identificarnos como un navegador normal y evitar bloqueos (Errores 406 / 403)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

# 1. PRUEBA API CLIMA (OpenWeather)
def probar_api_clima(api_key, ciudad="Madrid"):
    print("\n=== 1. PROBANDO API CLIMA (OpenWeather) ===")
    url = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={api_key}&units=metric&lang=es"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("✅ ¡ÉXITO! Conexión correcta con OpenWeather.")
            print(f"📍 Ubicación: {data['name']}")
            print(f"🌡️ Temperatura: {data['main']['temp']} °C")
            print(f"💨 Viento: {data['wind']['speed']} m/s, Dirección: {data['wind']['deg']}°")
            print(f"🌧️ Clima: {data['weather'][0]['description']}")
            return True
        elif response.status_code == 401:
            print("⏳ ATENCIÓN: Código 401. Si acabas de registrarte en OpenWeather, tu API Key tarda entre 1 y 2 horas en activarse por completo. Vuelve a probarla en un rato.")
            return False
        else:
            print(f"❌ ERROR: Código de estado {response.status_code}.")
            return False
    except Exception as e:
        print(f"❌ EXCEPCIÓN: Fallo de red - {e}")
        return False

# 2. PRUEBA API INCENDIOS (NASA FIRMS)
def probar_api_nasa_firms():
    print("\n=== 2. PROBANDO API INCENDIOS (NASA FIRMS) ===")
    # Endpoint público de descarga directa de focos de calor MODIS de los últimos 24h
    url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv"
    
    try:
        # Hacemos una petición ligera leyendo solo las primeras líneas
        response = requests.get(url, headers=HEADERS, stream=True, timeout=10)
        if response.status_code == 200:
            lineas = []
            for i, line in enumerate(response.iter_lines()):
                if i < 5:  # Leemos solo 5 líneas de muestra
                    lineas.append(line.decode('utf-8'))
                else:
                    break
            print("✅ ¡ÉXITO! Conexión correcta con NASA FIRMS.")
            print(f"📊 Encabezado del CSV de la NASA: {lineas[0]}")
            if len(lineas) > 1:
                print(f"📍 Muestra de foco detectado: {lineas[1]}")
            return True
        else:
            print(f"❌ ERROR: Código de estado {response.status_code} al consultar NASA FIRMS.")
            return False
    except Exception as e:
        print(f"❌ EXCEPCIÓN: Fallo de red - {e}")
        return False

# 3. PRUEBA MAPAS Y CARRETERAS (OpenStreetMap - Nominatim API)
def probar_api_openstreetmap(region="Comunidad de Madrid"):
    print("\n=== 3. PROBANDO API MAPAS (OpenStreetMap - Nominatim) ===")
    # Endpoint de geocodificación y detalles estructurados de OpenStreetMap
    url = f"https://nominatim.openstreetmap.org/search?q={region}&format=json&polygon_geojson=1"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if len(data) > 0:
                print("✅ ¡ÉXITO! Conexión correcta con OpenStreetMap (Nominatim).")
                print(f"🗺️ Bounding Box / Límites de {data[0]['display_name']}:")
                print(f"📍 Coordenadas límite (Lat/Lon): {data[0]['boundingbox']}")
                return True
            else:
                print("⚠️ No se encontraron resultados para la región.")
                return False
        else:
            print(f"❌ ERROR: Código de estado {response.status_code}.")
            return False
    except Exception as e:
        print(f"❌ EXCEPCIÓN: Fallo de red - {e}")
        return False

if __name__ == "__main__":
    print("🚀 INICIANDO COMPROBACIÓN DE APIS PARA EL TFM...")
    
    MY_OPENWEATHER_KEY = "a1e91a3b4df8083a74626946df2d5c9a" # Pon tu clave de OpenWeather
    
    probar_api_clima(MY_OPENWEATHER_KEY)
    probar_api_nasa_firms()
    probar_api_openstreetmap()