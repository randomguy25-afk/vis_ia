
import requests
import pandas as pd
import plotnine
from dagster import asset, Output, MetadataValue, AssetCheckResult, asset_check
import geopandas as gpd
from plotnine import ggplot, aes, geom_map, scale_fill_cmap, theme_void, labs
import subprocess
import os
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
    # 1. Preparar el entorno para ejecutar el código de la IA
    import plotnine
    entorno_ejecucion = {
        'pd': pd,
        'plt': plotnine,
        'islas_raw': islas_raw
    }
    entorno_ejecucion.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})

    try:
        # 2. Ejecutar el código generado por la IA
        exec(codigo_generado_ia, entorno_ejecucion)
        grafico = entorno_ejecucion['generar_plot'](islas_raw)

        # 3. Guardar la imagen en la carpeta raíz
        ruta_archivo = os.path.abspath("visualizacion_ia.png")
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)
        context.log.info(f"Imagen guardada en: {ruta_archivo}")

        # 4. AUTOMATIZACIÓN DE GIT (Punto 7 del PDF)
        import subprocess
        # Añadimos todo, hacemos commit y push
        subprocess.run(["git", "add", "."], check=True)
        
        # El commit puede fallar si no hay cambios, por eso capturamos el error
        try:
            subprocess.run(["git", "commit", "-m", "Actualización automática del gráfico"], check=True)
            subprocess.run(["git", "push", "origin", "practica-calidad-checks"], check=True) # Asegúrate que tu rama es 'main'
            context.log.info("¡Subida a GitHub completada con éxito!")
        except subprocess.CalledProcessError:
            context.log.info("No había cambios nuevos para subir.")

        # 5. El RETURN debe ir AL FINAL de todo
        return Output(
            value=ruta_archivo,
            metadata={
                "ruta": MetadataValue.path(ruta_archivo),
                "mensaje": "Gráfico generado y subido a GitHub"
            }
        )

    except Exception as e:
        context.log.error(f"Error en el proceso: {e}")
        raise e


# --- CHECKS ---

@asset_check(asset=codigo_generado_ia)
def check_codigo_valido(codigo_generado_ia):
    tiene_funcion = "def generar_plot" in codigo_generado_ia
    return AssetCheckResult(passed=tiene_funcion)

@asset_check(asset=islas_raw)
def check_datos_no_vacios(islas_raw):
    return AssetCheckResult(passed=len(islas_raw) > 0)