
from json.encoder import JSONEncoder
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from google.oauth2 import service_account

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

#BASE_DIR = os.path.abspath(os.path.dirname(__file__))

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
    key_path = "data-case-study-322621-419f15740599.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client()
    table_id = "data-case-study-322621.laraclink.crypto_currency"
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    # Construct a BigQuery client object.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("current_price_usd", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("current_price_brl", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("current_price_eur", "FLOAT", mode="REQUIRED"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = coin_result

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))                

def bitcoin_currency():
    bitcoin_requests = requests.get('https://api.coingecko.com/api/v3/coins/bitcoin').json()
    bitcoin = Coin(bitcoin_requests['id'], bitcoin_requests['symbol'], bitcoin_requests['name'], bitcoin_requests['market_data']['current_price']['usd'], bitcoin_requests['market_data']['current_price']['brl'], bitcoin_requests['market_data']['current_price']['eur']), 
    upload_bigquery(CoinEncoder().encode(bitcoin))
    return CoinEncoder().encode(bitcoin)

def ethereum_currency():
    ethereum_requests = requests.get('https://api.coingecko.com/api/v3/coins/ethereum').json()
    ethereum = Coin(ethereum_requests['id'], ethereum_requests['symbol'], ethereum_requests['name'], ethereum_requests['market_data']['current_price']['usd'], ethereum_requests['market_data']['current_price']['brl'], ethereum_requests['market_data']['current_price']['eur']), 
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