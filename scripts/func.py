from datetime import datetime 
import requests

def add_timestamp(func):
    def wrapper(*args):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_args = (f'[{timestamp}] - {args[0]}',)
        return func(*new_args)
    return wrapper


@add_timestamp
def bad_request_info(company, func):
    print(f'{company}: 400 bad request - function: {func.__name__}')


@add_timestamp
def print_with_timestamp(*args):
    print(*args)


def get_data(company: str, key: str):
    """ 
    Extract stock data from alphavantage
    :param company: name of company in stock way eg. GOOG (it's google stock)
    :param key: API KEY of https://www.alphavantage.co
    :return results: dict with basic info about stocks -> company: str, open: float, close:float, high: float, low: float, volume: int, stock_date: str
    """
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={company}&apikey={key}'
    r = requests.get(url)
    data = r.json()
    try:
        last_refreshed = data['Meta Data']['3. Last Refreshed']

    except KeyError:
        print_with_timestamp(f'{company}: 400 bad request - function: {get_data.__name__}')
        raise KeyError

    results = {
        'company': company,
        'open': float(data['Time Series (Daily)'][last_refreshed]['1. open']),
        'close': float(data['Time Series (Daily)'][last_refreshed]['4. close']),
        'high': float(data['Time Series (Daily)'][last_refreshed]['2. high']),
        'low': float(data['Time Series (Daily)'][last_refreshed]['3. low']),
        'volume': int(data['Time Series (Daily)'][last_refreshed]['5. volume']),
        'stock_date': last_refreshed
    }

    return results


def get_market_capitaliztation(company: str, key: str):
    """
    Extract information about Market Capitalization - coefficient assessing the company's value
    :param company: name of company in stock way eg. GOOG (it's google stock)
    :param key: API KEY of https://www.alphavantage.co
    :return results: Market Capitalization: int
    """

    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company}&apikey={key}'
    r = requests.get(url)
    data = r.json()

    try:
        results = int(data["MarketCapitalization"])
    except KeyError:
        print_with_timestamp(f'{company}: 400 bad request - function: {get_market_capitaliztation.__name__}')
        raise KeyError

    return results


def my_function():
    pass
