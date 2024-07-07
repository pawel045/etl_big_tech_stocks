from google.cloud import bigquery
from datetime import date, timedelta
import func
import pandas as pd
import requests
from scripts.secret_keys import AV_API_KEY, SERVICE_ACCOUNT_JSON

def main():
    # 0. DEPENDECIES
    api_key = AV_API_KEY
    service_account_json = SERVICE_ACCOUNT_JSON
    table_id = 'stocks-428319.tech_stocks.tech_big5'
    d = { # schema for pandas DataFrame
        'company': [],
        'open': [],
        'close': [],
        'high': [],
        'low': [],
        'MarketCapitalization': [],
        'stock_date': [],
        'volume': [],
    }

    # 1. EXTRACT
    func.print_with_timestamp('EXTRACTING DATA FROM SOURCE...')
    companies = ['GOOG', 'AMZN', 'AAPL', 'META', 'MSFT']
    
    for company in companies:
        try:
            result = func.get_data(company, api_key)
            market_capital = func.get_market_capitaliztation(company, api_key)
            for key, item in d.items():
                if key == 'MarketCapitalization':
                    item.append(market_capital)
                else:
                    item.append(result[key])
        except:
            continue
    df = pd.DataFrame(data=d)
    func.print_with_timestamp('EXTRACTING DONE')

    # 2. TRANSFORM - create new row with collecitve share value
    func.print_with_timestamp('TRANSFORMING DATA...')
    company = 'BIG5'
    coeff = df['MarketCapitalization']/1_000_000
    open = coeff*df['open']
    close = coeff*df['close']
    high = coeff*df['high']
    low = coeff*df['low']
    mc = df['MarketCapitalization']
    volume = df['volume']
    stock_date = df['stock_date']

    try:
        new_row = {
            'company': [company],
            'open': [open.mean()],
            'close': [close.mean()],
            'high': [high.mean()],
            'low': [low.mean()],
            'MarketCapitalization': [mc.sum()],
            'stock_date': [stock_date[0]],
            'volume': [volume.sum()]
            }
        df_new_row = pd.DataFrame(data=new_row)
        df = pd.concat([df, df_new_row])
    except Exception as err:
        func.print_with_timestamp('Cannot create dataframe. No extracted data.')
        raise
    func.print_with_timestamp('TRANSFORMING DONE')
    # 3. LOAD
    # Connection with BigQuery
    func.print_with_timestamp('LOADING DATE TO BIGQUERY...')
    client = bigquery.Client.from_service_account_json(service_account_json)
    

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
    job.results()
    
    table = client.get_table(table_id)
    func.print_with_timestamp(
        f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}'
    )
    func.print_with_timestamp('LOADING DONE')


if __name__=='__main__':
    func.print_with_timestamp('START ETL PROCESS')
    try:
        main()
        func.print_with_timestamp('ETL PROCESS COMPLETED SUCCESSFULLY')
    except:
        func.print_with_timestamp('ETL PROCESS FAILED')