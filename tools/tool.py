import json
from typing import Optional
import pandas as pd
import sqlite3
import re



def consulta_filtrada(tabla: str,condiciones: str) -> pd.DataFrame:
    """
    Realiza una consulta filtrada en una base de datos SQLite.
    """
    conn = sqlite3.connect('../inmuebles.db')
    #if isinstance(columnas, list):
     #   columnas = ", ".join(columnas)

    # Construir la consulta base
    consulta = f"SELECT * FROM {tabla}"
    print(f"Consulta base temp: {condiciones}")
    # Agregar filtros
    
    if condiciones:
        consulta += " WHERE "+ condiciones
    elif 'barrio_comun' in condiciones:
        patron = r"(barrio_comun)\s*=\s*"
        consulta = re.sub(patron, r"\1 LIKE ", consulta)

    print(f"Consulta generada: {consulta}")
    
    try:
        df = pd.read_sql_query(consulta, conn)
        return df
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
}]

def handle_tool_call(message):
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
    
    return response