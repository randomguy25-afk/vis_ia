import requests
import pandas as pd
import plotnine
from dagster import asset, Output, MetadataValue, AssetCheckResult, asset_check
import geopandas as gpd
from plotnine import ggplot, aes, geom_map, scale_fill_cmap, theme_void, labs
@asset
def islas_raw():
    """Carga el conjunto de datos de renta de Canarias"""
    return pd.read_csv("distribucion-renta-canarias.csv")

@asset
def template_ia(islas_raw):
    col_isla = "TERRITORIO#es"
    col_anio = "TIME_PERIOD#es"
    col_valor = "OBS_VALUE"
    col_medida = "MEDIDAS#es"

    template_tecnico = """
def generar_plot(df):
    # Código aquí
"""

    system_content = (
        "Eres un programador de Python experto en Plotnine. "
        "Tu respuesta debe contener ÚNICAMENTE el código Python, sin explicaciones ni saludos. "
        "No uses xlab() ni ylab() dentro de theme(). Usa labs()."
    )

    descripcion_grafico = f"""
    Basado en el dataframe 'islas_raw', completa la función 'generar_plot(df)':
    1. Filtra: '{col_medida}' == 'Sueldos y salarios'.
    2. Filtra: '{col_isla}' debe ser 'Tenerife' o 'Gran Canaria'.
    3. Filtra: '{col_anio}' debe ser 2020, 2021 o 2022.
    4. Proceso: Convierte '{col_anio}' a string.
    5. Gráfico: 
       - aes(x='{col_anio}', y='{col_valor}', color='{col_isla}', group='{col_isla}')
       - geom_line(size=1.5) + geom_point(size=3)
       - scale_color_manual(values={{'Tenerife': '#003366', 'Gran Canaria': '#D3D3D3'}})
       - labs(title='Evolución de Sueldos y Salarios (2020-2022)', x='Año', y='Euros')
       - theme_minimal() + theme(legend_position='bottom')
    """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": descripcion_grafico}
        ],
        "temperature": 0.1,
        "stream": False
    }

@asset
def codigo_generado_ia(template_ia):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    
    response = requests.post(url, json=template_ia, headers=headers)
    response.raise_for_status()
    
    codigo = response.json()['choices'][0]['message']['content']
    
    # --- NUEVA LIMPIEZA ROBUSTA ---
    # Si la IA escribe texto antes del código, buscamos dónde empieza la función
    if "def generar_plot" in codigo:
        codigo = codigo[codigo.find("def generar_plot"):]
    
    # Limpiamos marcas de Markdown
    codigo_limpio = codigo.replace("```python", "").replace("```", "").strip()
    return codigo_limpio

@asset
def visualizacion_png(context, codigo_generado_ia, islas_raw):
    # Preparamos el entorno con las librerías necesarias
    entorno_ejecucion = globals().copy()
    entorno_ejecucion['pd'] = pd
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')
    })

    try:
        # Ejecutamos el código generado por la IA
        exec(codigo_generado_ia, entorno_ejecucion)
        
        # Llamamos a la función que la IA definió
        grafico = entorno_ejecucion['generar_plot'](islas_raw)
        
        ruta_archivo = "visualizacion_ia.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)
        
        return Output(
            value=ruta_archivo,
            metadata={
                "ruta": MetadataValue.path(ruta_archivo),
                "mensaje": "Gráfico generado exitosamente por la IA"
            }
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e

@asset
def mapa_renta_municipios_final():
    # 1. Load CSV and format the code to 5 digits (e.g., "35001")
    df = pd.read_csv("datos_2023.csv")
    df['TERRITORIO_CODE'] = (
        df['TERRITORIO_CODE']
        .astype(str)
        .str.split('.').str[0]
        .str.strip()
        .str.zfill(5)
    )

    # 2. Load the GeoJSON
    # Note: Ensure the filename is correct (removed the double .json.json)
    gdf = gpd.read_file("mapa_geo.json")
    
    # 3. Use 'codigo' from the properties, NOT 'id'
    columna_geo = 'geocode' 
    
    if columna_geo not in gdf.columns:
        # Debugging helper: if it fails, this tells you what columns actually exist
        raise ValueError(f"Column '{columna_geo}' not found. Available: {gdf.columns.tolist()}")

    gdf[columna_geo] = (
        gdf[columna_geo]
        .astype(str)
        .str.strip()
        .str.zfill(5)
    )

    # 4. Merge
    mapa_data = gdf.merge(df, left_on=columna_geo, right_on="TERRITORIO_CODE", how='inner')

    print(f"DEBUG: Se han unido {len(mapa_data)} municipios correctamente.")

    # 5. Plot
    plot = (
        ggplot(mapa_data)
        + geom_map(aes(fill='OBS_VALUE'))
        + scale_fill_cmap(cmap_name='YlGnBu') 
        + theme_void() 
        + labs(
            title="Sueldos y Salarios por Municipio",
            subtitle="Canarias - 2023",
            fill="Euros"
        )
    )

    ruta_salida = "mapa_canarias_final.png"
    plot.save(ruta_salida, width=12, height=8, dpi=150)
    
    return ruta_salida

# --- CHECKS ---

@asset_check(asset=codigo_generado_ia)
def check_codigo_valido(codigo_generado_ia):
    tiene_funcion = "def generar_plot" in codigo_generado_ia
    return AssetCheckResult(passed=tiene_funcion)

@asset_check(asset=islas_raw)
def check_datos_no_vacios(islas_raw):
    return AssetCheckResult(passed=len(islas_raw) > 0)