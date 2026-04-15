import os
from dagster import (
    Definitions, 
    define_asset_job, 
    AssetSelection, 
    sensor, 
    RunRequest,
    load_assets_from_modules
)
# Suponiendo que tus assets están en el módulo actual
import assets_ia 

# Definimos el Job que materializa todos los assets del pipeline
pipeline_rentas_job = define_asset_job(
    name="pipeline_rentas_job", 
    selection=AssetSelection.all()
)

# Sensor que vigila el cambio en el CSV
@sensor(job=pipeline_rentas_job)
def monitor_csv_sensor(context):
    ruta_fichero = "distribucion-renta-canarias.csv"
    
    if os.path.exists(ruta_fichero):
        # Obtenemos la fecha de última modificación
        mtime = str(os.path.getmtime(ruta_fichero))
        # El cursor guarda el estado de la última vez que se ejecutó
        last_mtime = context.cursor or "0"

        if mtime != last_mtime:
            context.update_cursor(mtime)
            # Disparamos la ejecución
            yield RunRequest(run_key=mtime)

# Carga todos los assets y checks del archivo assets_ia.py
all_assets = load_assets_from_modules([assets_ia])

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        assets_ia.check_codigo_valido,
        assets_ia.check_datos_no_vacios
    ],
    sensors=[monitor_csv_sensor],
    jobs=[pipeline_rentas_job]
)