import json
from typing import Optional, List, Union
import pandas as pd
import sqlite3
from pydantic import BaseModel

# Definición del modelo Pydantic para validación de parámetros
class SQLiteQueryParams(BaseModel):
    tabla: str
    columnas: Union[str, List[str]] = "*"
    condiciones: Optional[str] = None

def consulta_filtrada(
    tabla: str,
    columnas: Union[str, List[str]] = "*",
    condiciones: Optional[str] = None
) -> pd.DataFrame:
    """
    Realiza una consulta filtrada en una base de datos SQLite.
    """
    conn = sqlite3.connect('../inmuebles.db')
    if isinstance(columnas, list):
        columnas = ", ".join(columnas)
    
    consulta = f"SELECT {columnas} FROM {tabla}"
    if condiciones:
        consulta += f" WHERE {condiciones}"
    
    try:
        df = pd.read_sql_query(consulta, conn)
        return df
    except Exception as e:
        print(f"Error al realizar la consulta: {e}")
        return pd.DataFrame()

# Definición de la herramienta para OpenAI
sqlite_tool = [{
    "type": "function",
    "function": {
        "name": "consulta_filtrada",
        "description": "Realiza consultas filtradas en una base de datos SQLite",
        "parameters": {
            "type": "object",
            "properties": {
                "tabla": {
                    "type": "string",
                    "description": "Nombre de la tabla a consultar",
                    "default":"property"
                },
                "columnas": {
                    "type": ["string", "array"],
                    "items": {
                        "type": "string"
                    },
                    "description": "Columnas a seleccionar. Puede ser '*' o una lista de nombres de columnas",
                    "default": "*"
                },
                "condiciones": {
                    "type": "string",
                    "description": """Condiciones SQL para filtrar las filas (ejemplo: SELECT * FROM property WHERE baños =3 AND
                    barrio_comun = 'las quintas' AND baños =3 AND estrato= 4 AND area_construida > 200 AND estado = AND (estado = 'venta_apartamento' OR estado = 'venta_casa'))""",
                    "default": None
                }
            },
            "required": ["tabla"]
        }
    }
}]

def handle_tool_call(message):
    tool_call = message.tool_calls[0]
    arguments = json.loads(tool_call.function.arguments)
    print(arguments)
    tabla = arguments.get('tabla')
    columnas = arguments.get('columnas')
    condiciones = arguments.get('condiciones')
    dataframe = consulta_filtrada(tabla,columnas,condiciones)
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