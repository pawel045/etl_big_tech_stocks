from datetime import date, timedelta
import func
import pandas as pd
import requests
from source_data_api_key import AV_API_KEY

def main():
    # 0. DEPENDECIES
    api_key = AV_API_KEY
    d = {
        'company': [],
        'open': [],
        'close': [],
        'high': [],
        'low': [],
        'volume': [],
        'MarketCapitalization': [],
        'stock_date': []
    }

    # 1. EXTRACT
    companies = ['GOOG', 'AMZN', 'AAPL', 'META', 'MSFT']
    
    for company in companies:
        try:
            result = func.get_data(company, api_key)
            market_capital = func.get_market_capitaliztation(company, api_key)
            
            for key, item in d.items():
                if key != 'MarketCapitalization':
                    item.append(market_capital)
                else:
                    item.append(result[key])

        except:
            continue
    

    # d = { # tylko do testow - usnunac
    # 'company': ['GOOG', 'AMZN'],
    # 'open': [100, 200],
    # 'close': [105, 210],
    # 'high': [110, 220],
    # 'low': [90, 180],
    # 'volume': [1_000, 2_000],
    # 'MarketCapitalization': [1_000_000, 2_000_000],
    # 'stock_date': ['2024-10-10', '2024-10-10']
    # }
    df = pd.DataFrame(data=d)

    # 2. TRANSFORM - create new row with collecitve share value
    company = 'BIG5'
    coeff = df['MarketCapitalization']/1_000_000
    open = coeff*df['open']
    close = coeff*df['close']
    high = coeff*df['high']
    low = coeff*df['low']
    mc = df['MarketCapitalization']
    volume = df['volume']
    stock_date = df['stock_date']

    new_row = {
        'company': [company],
        'open': [open.mean()],
        'close': [close.mean()],
        'high': [high.mean()],
        'low': [low.mean()],
        'volume': [volume.sum()],
        'MarketCapitalization': [mc.sum()],
        'stock_date': [stock_date[0]]
    }
    df_new_row = pd.DataFrame(data=new_row)
    df = pd.concat([df, df_new_row])



    # 3. LOAD


if __name__=='__main__':
    main()
