import pandas as pd
from google.cloud import bigquery

d = {
    'company': ['TEST'],
    'open': [10],
    'close': [10],
    'high': [10],
    'low': [10],
    'MarketCapitalization': [10],
    'stock_date': ['1900-01-01'],
    'volume': [100],
}

df = pd.DataFrame(d)
service_account_json = 'stocks-428319-a9e884842218.json'

client = bigquery.Client.from_service_account_json(service_account_json)
table_id = f'{client.project}.tech_stocks.tech_big5'

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('company', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('open', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('close', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('high', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('low', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('MarketCapitalization', bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField('stock_date', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('volume', bigquery.enums.SqlTypeNames.INTEGER),
    ]
)
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

# Print results
table = client.get_table(table_id)
print(
    f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}'
)
