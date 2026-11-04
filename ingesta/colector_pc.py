"""Colector en la PC: consulta OpenSky en vivo cada 2 minutos, publica en Pub/Sub y guarda la grabación.

OpenSky bloquea las IP de los grandes proveedores de nube; desde la PC sí responde.
Uso:  .venv\\Scripts\\python ingesta\\colector_pc.py [minutos]   (por defecto corre 60 minutos)
"""
import sys
import time

import main

minutos = int(sys.argv[1]) if len(sys.argv) > 1 else 60
fin = time.time() + minutos * 60
while time.time() < fin:
    inicio = time.time()
    try:
        lote = main.leer_opensky()
        main.grabar(lote)
        main.publicar(lote, "opensky")
    except Exception as e:  # una consulta fallida no detiene al colector
        print(f"error en la consulta: {e}")
    time.sleep(max(0, 120 - (time.time() - inicio)))
