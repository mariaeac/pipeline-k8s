from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import json

# Conexão com o Postgres que está no namespace dados
DB_CONFIG = {
    "host": "postgres-service.dados.svc.cluster.local",
    "port": 5432,
    "database": "pipeline",
    "user": "airflow",
    "password": "airflow123"
}

def extract_data(**context):
    """
    Call API 
    """
    url = "https://brasil.io/api/dataset/covid19/caso_full/data/"
    params = {
        "place_type": "state",
        "is_last": "True",
    }
    headers = {
        "User-Agent": "pipeline-k8s-estudo/1.0"
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    dados = response.json()["results"]
    print(f"Extraídos {len(dados)} registros")

    context["ti"].xcom_push(key="dados_covid", value=dados)

def load_postgres(**context):
    dados = context["ti"].xcom_pull(key="dados_covid", task_ids="extrai_dados")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Cria a tabela se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS covid_raw (
            state           VARCHAR(2),
            date            DATE,
            new_cases       INTEGER,
            new_deaths      INTEGER,
            total_cases     INTEGER,
            total_deaths    INTEGER,
            inserted_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    # Limpa dados anteriores pra evitar duplicatas
    cursor.execute("DELETE FROM covid_raw")

    # Insere os novos dados
    for registro in dados:
        cursor.execute("""
            INSERT INTO covid_raw (
                state, date, new_cases, new_deaths,
                total_cases, total_deaths
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            registro.get("state"),
            registro.get("date"),
            registro.get("new_confirmed"),
            registro.get("new_deaths"),
            registro.get("last_available_confirmed"),
            registro.get("last_available_deaths"),
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Carregados {len(dados)} registros no Postgres")


with DAG(
    dag_id="covid_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["covid", "brasil.io", "estudo"],
) as dag:

    extrai = PythonOperator(
        task_id="extrai_dados",
        python_callable=extrai_dados,
    )

    carrega = PythonOperator(
        task_id="carrega_postgres",
        python_callable=carrega_postgres,
    )

    extrai >> carrega
