import requests
import json

endpoints = {
    "1. OpenStreetMap Overpass (vías y tráfico en Madrid)": {
        "url": "https://overpass-api.de/api/interpreter",
        "method": "POST",
        "data": {"data": "[out:json][timeout:25];node['highway'](40.3,-3.8,40.5,-3.5);out 5;"},
        "headers": {"User-Agent": "TFM_Mobility_App/1.0 (contact: pasobrad@ucm.es)"}
    },
    "2. Generalitat de Catalunya - Incidencias Tráfico (GeoJSON Live)": {
        "url": "https://movilidad.cit.gva.es/api/v1/incidencias.json",
        "method": "GET",
        "data": None,
        "headers": {"User-Agent": "Mozilla/5.0"}
    }
}

print("📡 Probando conexión con cabecera User-Agent personalizada...\n")

for name, cfg in endpoints.items():
    print(f"Testing {name}...")
    try:
        if cfg["method"] == "POST":
            r = requests.post(cfg["url"], data=cfg["data"], headers=cfg["headers"], timeout=10)
        else:
            r = requests.get(cfg["url"], headers=cfg["headers"], timeout=10)
            
        print(f"   --> STATUS CODE: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            items = data.get("elements") or data.get("features") or data
            print(f"   ✅ ¡CONEXIÓN EXITOSA (200 OK)! Elementos recibidos: {len(items) if isinstance(items, list) else 'OK'}")
            if isinstance(items, list) and len(items) > 0:
                print("   📍 Muestra:", json.dumps(items[0], ensure_ascii=False)[:250])
            print("="*50 + "\n")
        else:
            print(f"   ❌ Fallo con código HTTP {r.status_code}\n")
    except Exception as e:
        print(f"   💥 Error: {e}\n")