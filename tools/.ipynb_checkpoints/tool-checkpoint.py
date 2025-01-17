import pandas as pd
import numpy as np
import sqlite3
import os


def registro(name,last_name,codigo_inmueble,precio_inmueble):
    conn = sqlite3.connect('registro.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS registro(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    codigo_inmueble TEXT NOT NULL,
    precio_inmueble INTEGER NOT NULL
    )
    ''')
    cursor.execute('''
    INSERT INTO registro(name,last_name,codigo_inmueble,precio_inmueble)
    VALUES(?,?,?,?)
    ''',(name,last_name,codigo_inmueble,precio_inmueble))
    conn.commit()
    conn.close()
    return 'Registro exitoso'