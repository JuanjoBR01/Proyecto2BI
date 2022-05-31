def crear_tablas():
    return """

        DROP TABLE IF EXISTS fact_dmgrfc, fact_salud, fact_edcn, fact_salubridad;
        DROP TABLE IF EXISTS tabla_municipio, tabla_fecha, tabla_indicador;

        CREATE TABLE IF NOT EXISTS tabla_municipio(
            llave_municipio INT PRIMARY KEY,
            departamento VARCHAR(50),
            nombre_municipio VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS tabla_fecha(
            llave_fecha DATE PRIMARY KEY,
            dia_valor INT,
            mes_valor INT,
            mes_texto VARCHAR(10),
            anio INT
        );

        CREATE TABLE IF NOT EXISTS tabla_indicador(
            llave_indicador INT PRIMARY KEY,
            subcategoria VARCHAR(250),
            nombre_indicador VARCHAR(250)
        );

        CREATE TABLE IF NOT EXISTS fact_dmgrfc(
            llave_dmgrfc INT PRIMARY KEY,
            fk_municipio INT REFERENCES tabla_municipio(llave_municipio),
            fk_indicador INT REFERENCES tabla_indicador(llave_indicador),
            fk_fecha DATE REFERENCES tabla_fecha(llave_fecha),
            valor_dmgrfc DECIMAL
        );

        CREATE TABLE IF NOT EXISTS fact_salud(
            llave_salud INT PRIMARY KEY,
            fk_municipio INT REFERENCES tabla_municipio(llave_municipio),
            fk_indicador INT REFERENCES tabla_indicador(llave_indicador),
            fk_fecha DATE REFERENCES tabla_fecha(llave_fecha),
            valor_salud DECIMAL
        );

        CREATE TABLE IF NOT EXISTS fact_edcn(
            llave_edcn INT PRIMARY KEY,
            fk_municipio INT REFERENCES tabla_municipio(llave_municipio),
            fk_indicador INT REFERENCES tabla_indicador(llave_indicador),
            fk_fecha DATE REFERENCES tabla_fecha(llave_fecha),
            valor_edcn DECIMAL
        );

        CREATE TABLE IF NOT EXISTS fact_salubridad(
            llave_salubridad INT PRIMARY KEY,
            fk_municipio INT REFERENCES tabla_municipio(llave_municipio),
            fk_indicador INT REFERENCES tabla_indicador(llave_indicador),
            fk_fecha DATE REFERENCES tabla_fecha(llave_fecha),
            valor_salubridad DECIMAL
        );
    """
