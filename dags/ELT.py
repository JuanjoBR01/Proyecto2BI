# Utilidades de airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost_proyecto2", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )

    # task: 2 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 2.1 poblar tabla municipio
        poblar_municipio = PostgresOperator(
            task_id="poblar_municipio",
            postgres_conn_id='postgres_localhost_proyecto2',
            sql=insert_query_municipio(csv_path = "dimension_municipio")
        )

        # task: 2.2 poblar tabla indicador
        poblar_indicador = PostgresOperator(
            task_id="poblar_indicador",
            postgres_conn_id='postgres_localhost_proyecto2',
            sql=insert_query_indicador(csv_path ="dimension_indicador")
        )

        # task: 2.3 poblar tabla fecha
        poblar_fecha = PostgresOperator(
            task_id="poblar_fecha",
            postgres_conn_id='postgres_localhost_proyecto2',
            sql=insert_query_fecha(csv_path = "dimension_fecha")
        )
    
    with TaskGroup('poblar_tablas_de_hechos') as poblar_tablas_de_hechos:

        # task: 3.1 poblar la table de hechos de demografía
        poblar_fact_order_dmgrfc = PostgresOperator(
                task_id="poblar_hecho_dmgrfc",
                postgres_conn_id='postgres_localhost_proyecto2',
                sql=insert_query_dmgrfc(csv_path = "fact_dmgrfc")
        )

        # task: 3.2 poblar la table de hechos de salud
        poblar_fact_order_salud = PostgresOperator(
                task_id="poblar_hecho_salud",
                postgres_conn_id='postgres_localhost_proyecto2',
                sql=insert_query_salud(csv_path = "fact_salud")
        )

        # task: 3.3 poblar la table de hechos de educación
        poblar_fact_order_edcn = PostgresOperator(
                task_id="poblar_hecho_edcn",
                postgres_conn_id='postgres_localhost_proyecto2',
                sql=insert_query_edcn(csv_path = "fact_edcn")
        )

        # task: 3.3 poblar la table de hechos de salubridad
        poblar_fact_order_salubridad = PostgresOperator(
                task_id="poblar_hecho_salubridad",
                postgres_conn_id='postgres_localhost_proyecto2',
                sql=insert_query_salubridad(csv_path = "fact_salubridad")
        )
        
    # flujo de ejecución de las tareas  
    crear_tablas_db >> poblar_tablas_dimensiones >> poblar_tablas_de_hechos
