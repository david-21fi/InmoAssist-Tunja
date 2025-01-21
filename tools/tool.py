import json
from typing import Optional
import pandas as pd
import sqlite3
import re
import requests
from bs4 import BeautifulSoup

class Website():
    """
    Una clase de utilidad para representar un sitio web que hemos scrappeado
    """
    url: str
    title: str
    text: str

    def __init__(self, url):
        self.url = url
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        self.title = soup.title.string if soup.title else "No title found"
        for irrelevant in soup.body(["script", "style", "img", "input"]):
            irrelevant.decompose()
        self.text = soup.body.get_text(separator="\n", strip=True)


def consulta_filtrada(tabla: str,condiciones: str) -> pd.DataFrame:
    """
    Realiza una consulta filtrada en una base de datos SQLite.
    
    Args:
    tabla(str) : nombre de la tabla a consultar
    condiciones(str): condiciones en formato sql para realizar la consulta

    Return:
    df(DataFrame) : devuelve un dataframe con las observaciones encontradas
    """

    conn = sqlite3.connect('../inmuebles.db')
    
    # Construir la consulta base
    consulta = f"SELECT * FROM {tabla}"
    print(f"Consulta base temp: {condiciones}")
    # Agregar filtros
    
    if 'barrio_comun' in condiciones:
        patron = r"(barrio_comun)\s*=\s*"
        condiciones = re.sub(patron, r"\1 LIKE ", condiciones)

    consulta += " WHERE " + condiciones

    print(f"Consulta generada: {consulta}")
    
    try:
        df = pd.read_sql_query(consulta, conn)
        return df
    except Exception as e:
        print(f"Error al realizar la consulta: {e}")


#__________________________________________________
def resumen_inmueble(codigo:str):
    """
    Realiaza un Scrapeo del inmueble.

    Args:
    codigo(str): recibe el codigo el inmueble.

    Return

    """
    conn = sqlite3.connect('../inmuebles.db')
    ## consulta
    consulta = f"SELECT url FROM urls WHERE codigo_inmueble = '{codigo}'"

    
    try:
        df = pd.read_sql_query(consulta, conn)
        info = Website(df['url'].values[0])
        text = info.title + info.text
        return text
    except Exception as e:
        print(f"Error al realizar la consulta: {e}")

    

# Definición de la herramienta para OpenAI
sqlite_tool = [{
    "type": "function",
    "function": {
        "name": "consulta_filtrada",
        "description": """Realiza consultas filtradas en una base de datos SQLite utilinzo operadores logicos entre otros""",
        "parameters": {
            "type": "object",
            "properties": {
                "tabla": {
                    "type": "string",
                    "description": "Nombre de la tabla en la base de datos",
                    "default":'property'
                },
                "condiciones": {
                    "type": "string",
                    "description": """Condiciones SQL para filtrar los campos (ejemplo: SELECT * FROM property WHERE banos = 3 AND barrio_comun= '%el lago%'
                     AND baños =3 AND estrato= 4 AND area_construida > 200  AND (estado = 'venta_apartamento' OR estado = 'venta_casa')), los valores de barrio_comun proporcionalos en minuscula""",
                    "default": None
                }
            },
            "required": ["tabla"],
            "additionalProperties": False
        }
    }
},
{
    "type":"function",
    "function":{
        "name":"resumen_inmueble",
        "description":"""Consigue el contenido del inmuble que se encuetra el la página Web, llama a esta función siempre quel usuario necesites
        una descripción mas detallada del inmueble, por ejemplo si el cliete te pide ¿me puedes dar una descripcion mas detallada de este inmueble?.
        """,
        "parameters":{
            "type":"object",
            "properties":{
                "codigo":{
                    "type":"string",
                    "description": """El cogido del inmueble por ejemplo, codigo_inmueble = '16742-M4795726' """,
                    "default":None
                }
            },
            "required":["codigo"]
        }

    }

}]

def handle_tool_call(message):
    print(message.tool_calls[0],end='\n')
    print('Nombre de la herramienta: '+ message.tool_calls[0].function.name)
    if message.tool_calls[0].function.name == 'consulta_filtrada':
        tool_call = message.tool_calls[0]
        arguments = json.loads(tool_call.function.arguments)
        print(f"los argumentos son \n{arguments}")
        tabla = arguments.get('tabla')
        condiciones = arguments.get('condiciones') 
        dataframe = consulta_filtrada(tabla,condiciones)
        if dataframe is not None:
            response = {
                "role": "tool",
                "content": dataframe.to_json(orient='records'),
                "tool_call_id": message.tool_calls[0].id
            }
        else:
            response = {
                "role": "tool",
                "content": json.dumps({"error": "No se encontro considencias."}),
                "tool_call_id": message.tool_calls[0].id
            }
    elif message.tool_calls[0].function.name == 'resumen_inmueble':
        tool_call = message.tool_calls[0]
        arguments = json.loads(tool_call.function.arguments)
        print(f"los argumentos son \n{arguments}")
        codigo = arguments.get('codigo')
        texto = str(resumen_inmueble(codigo))
        if texto is not None:
            response = {
                "role": "tool",
                "content": texto,
                "tool_call_id": message.tool_calls[0].id
            }
        else:
            response = {
                "role": "tool",
                "content": json.dumps({"error": "No se encontro considencias."}),
                "tool_call_id": message.tool_calls[0].id
            }
    
    return response
  