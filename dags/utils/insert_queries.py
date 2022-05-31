from utils.file_util import cargar_datos

# Inserción del municipio
def insert_query_municipio(**kwargs):
    insert = f"INSERT INTO tabla_municipio (llave_municipio, departamento, nombre_municipio) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_municipio},\'{row.departamento}\',\'{row.nombre_municipio}\');\n"
    return insertQuery

# Inserción de la fecha
def insert_query_fecha(**kwargs):
    insert = f"INSERT INTO tabla_fecha (llave_fecha, dia_valor, mes_valor, mes_texto, anio) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(\'{row.llave_fecha}\',{row.dia_valor},{row.mes_valor}, \'{row.mes_texto}\',{row.anio});\n"
    return insertQuery


# Inserción del indicador
def insert_query_indicador(**kwargs):
    insert = f"INSERT INTO tabla_indicador (llave_indicador, subcategoria, nombre_indicador) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_indicador},\'{row.subcategoria}\',\'{row.nombre_indicador}\');\n"
    return insertQuery
    


# Inserción de los indicadores demográficos
def insert_query_dmgrfc(**kwargs):
    insert = f"INSERT INTO fact_dmgrfc (llave_dmgrfc, fk_municipio, fk_indicador, fk_fecha, valor_dmgrfc) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_dmgrfc},{row.fk_municipio},{row.fk_indicador}, \'{row.fk_fecha}\', {row.valor_dmgrfc});\n"
    return insertQuery


# Inserción de los indicadores de salud
def insert_query_salud(**kwargs):
    insert = f"INSERT INTO fact_salud (llave_salud, fk_municipio, fk_indicador, fk_fecha, valor_salud) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_salud},{row.fk_municipio},{row.fk_indicador}, \'{row.fk_fecha}\', {row.valor_salud});\n"
    return insertQuery


# Inserción de los indicadores de edcn
def insert_query_edcn(**kwargs):
    insert = f"INSERT INTO fact_edcn (llave_edcn, fk_municipio, fk_indicador, fk_fecha, valor_edcn) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_edcn},{row.fk_municipio},{row.fk_indicador}, \'{row.fk_fecha}\', {row.valor_edcn});\n"
    return insertQuery


# Inserción de los indicadores de salubridad
def insert_query_salubridad(**kwargs):
    insert = f"INSERT INTO fact_salubridad (llave_salubridad, fk_municipio, fk_indicador, fk_fecha, valor_salubridad) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.llave_salubridad},{row.fk_municipio},{row.fk_indicador}, \'{row.fk_fecha}\', {row.valor_salubridad});\n"
    return insertQuery
