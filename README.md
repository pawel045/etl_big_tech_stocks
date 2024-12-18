# ETL Big Tech Stocks

This project implements an ETL (Extract, Transform, Load) pipeline for processing and analyzing stock data of major technology companies, including Google, Amazon, Apple, Meta, and Microsoft. The pipeline is designed to collect daily stock data, transform it, and load it into a database or other data storage solutions for further analysis.

## Example results
### Tech Big 5 Report 10-2024

| stock_date  | google  | amazon  | apple  | meta    | microsoft |
|-------------|---------|---------|--------|---------|-----------|
| 2024-10-01  | 172.3   | 202.8   | 243.1  | 617.4   | 432.7     |
| 2024-10-02  | 168.7   | 198.5   | 239.9  | 622.6   | 428.3     |
| 2024-10-03  | 170.1   | 201.2   | 240.4  | 619.8   | 431.5     |
| 2024-10-04  | 169.9   | 200.7   | 238.6  | 623.1   | 429.2     |

<br>

## Setup

To get started with this project, you'll need to install the necessary dependencies.

1. Clone the repository:

    ```bash
    git clone https://github.com/pawel045/etl_big_tech_stocks.git
    cd etl_big_tech_stocks

2. Create and activate a virtual environment:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt

4. Inside <b>./etl/scripts</b> folder create file <b>secret_keys.py</b> with conent:

    ```python
    AV_API_KEY = '' # API KEY for https://www.alphavantage.co service
    SERVICE_ACCOUNT_JSON =r'' # credentials json from GCP for connecting with bigquery table

## Running the ETL pipeline

The ETL process is managed by Apache Airflow and runs daily. To trigger the ETL pipeline manually, follow these steps:

1. Initialize the Airflow database:

    ```bash
    airflow db init

2. Start the Airflow scheduler and webserver:

    ```bash
    airflow scheduler
    airflow webserver

3. Set up path folder with dags.
    
4. Access the Airflow web interface at `http://localhost:8080` and trigger the `big5_etl` DAG.

### Running the ETL script manually

If you prefer to run the ETL script without Airflow, you can do so using the following command:

  ``` bash
  python etl/scripts/etl.py
  ```

## License

MIT License
Copyright (c) 2024 Paweł Roszczyk
