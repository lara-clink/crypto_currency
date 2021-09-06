
from json.encoder import JSONEncoder
import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/laraclink/airflow/dags/data-case-study-322621-419f15740599.json"


from google.cloud import bigquery

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='crypto_currency',
    default_args=args,
    schedule_interval='0 7 * * *',
    start_date=days_ago(2),
) as dag:

    class Coin:
        def __init__(self, id, symbol, name, usd, brl, eur):
            self.id = id
            self.symbol = symbol
            self.name = name
            self.snapshot_date = datetime.today().strftime('%Y-%m-%d')
            self.current_price_usd = usd
            self.current_price_brl = brl
            self.current_price_eur = eur

    class CoinEncoder(JSONEncoder):
            def default(self, o):
                return o.__dict__

def upload_bigquery(coin_result):
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client()
    table_id = "crypto_currency"
    client = bigquery.Client(credentials=credentials, project= credentials.project_id)

    coin_result = coin_result.replace("[", "").replace("]", "").replace(" ", "")
    with open("coin_result.json", "w") as outfile:
        outfile.write(coin_result)

    filename = '/home/laraclink/airflow/dags/coin_result.json'
    dataset_id = 'laraclink'

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="us",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))            

def bitcoin_currency():
    bitcoin_requests = requests.get('https://api.coingecko.com/api/v3/coins/bitcoin').json()
    bitcoin = Coin(bitcoin_requests['id'], bitcoin_requests['symbol'], bitcoin_requests['name'], float(bitcoin_requests['market_data']['current_price']['usd']), float(bitcoin_requests['market_data']['current_price']['brl']), float(bitcoin_requests['market_data']['current_price']['eur'])), 
    upload_bigquery(CoinEncoder().encode(bitcoin))
    return CoinEncoder().encode(bitcoin)

def ethereum_currency():
    ethereum_requests = requests.get('https://api.coingecko.com/api/v3/coins/ethereum').json()
    ethereum = Coin(ethereum_requests['id'], ethereum_requests['symbol'], ethereum_requests['name'], float(ethereum_requests['market_data']['current_price']['usd']), float(ethereum_requests['market_data']['current_price']['brl']), float(ethereum_requests['market_data']['current_price']['eur'])), 
    upload_bigquery(CoinEncoder().encode(ethereum))
    return CoinEncoder().encode(ethereum)


start = DummyOperator(
    task_id='start',
)

task_bitcoin = PythonOperator(
    task_id = 'bitcoin_currency', 
    python_callable = bitcoin_currency,
    dag = dag
)

task_ethereum = PythonOperator(
    task_id = 'ethereum_currency', 
    python_callable = ethereum_currency,
    dag = dag
)

end = DummyOperator(
    task_id='end',
)

start >> task_bitcoin >> task_ethereum >> end