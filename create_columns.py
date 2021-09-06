from google.oauth2 import service_account
from google.cloud import bigquery

key_path = "data-case-study-322621-419f15740599.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client()
table_id = "data-case-study-322621.laraclink.crypto_currency"

schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("current_price_usd", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("current_price_brl", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("current_price_eur", "FLOAT", mode="NULLABLE"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)