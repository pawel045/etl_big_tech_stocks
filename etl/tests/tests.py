import pytest
import requests
import requests_mock
import scripts.func as func


def test_get_data_success():
    company = "GOOG"
    key = "test_key"
    mock_response = {
        "Meta Data": {
            "3. Last Refreshed": "2023-06-22"
        },
        "Time Series (Daily)": {
            "2023-06-22": {
                "1. open": "135.67",
                "4. close": "137.45",
                "2. high": "138.00",
                "3. low": "134.12",
                "5. volume": "1523421"
            }
        }
    }

    with requests_mock.Mocker() as m:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={company}&apikey={key}'
        m.get(url, json=mock_response)

        result = func.get_data(company, key)
        
        assert result['company'] == company
        assert result['open'] == 135.67
        assert result['close'] == 137.45
        assert result['high'] == 138.00
        assert result['low'] == 134.12
        assert result['volume'] == 1523421
        assert result['stock_date'] == "2023-06-22"


def test_get_data_failure():
    company = "GOOG"
    key = "test_key"
    mock_response = {
        "Error Message": "Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for TIME_SERIES_DAILY."
    }

    with requests_mock.Mocker() as m:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={company}&apikey={key}'
        m.get(url, json=mock_response, status_code=400)

        with pytest.raises(KeyError):
            func.get_data(company, key)


def test_get_market_capitalization_success():
    company = 'GOOG'
    key = 'test_key'
    mock_resposne = {
        "MarketCapitalization": "1000000000"
    }

    with requests_mock.Mocker() as m:
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company}&apikey={key}'
        m.get(url, json=mock_resposne)
        result = func.get_market_capitaliztation(company, key)

        assert result == 1_000_000_000


def test_get_market_capitalization_failure():
    company = "GOOG"
    key = "test_key"
    mock_response = {
        "Error Message": "Invalid API call."
    }

    with requests_mock.Mocker() as m:
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={company}&apikey={key}'
        m.get(url, json=mock_response, status_code=400)

        with pytest.raises(KeyError):
            func.get_market_capitaliztation(company, key)
