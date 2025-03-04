# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='DHK_WH',
        database='dev'
    )
    # Create a cursor object
    return conn.cursor()


@task
def extract(symbol):
    vantage_api_key = Variable.get('vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}&outputsize=compact'

    r = requests.get(url)
    data = r.json()

    results = []  # List to store stock data
    cutoff_date = datetime.now() - timedelta(days=90)  # Calculate the 90-day cutoff

    for d in sorted(data["Time Series (Daily)"], reverse=True):  # Ensure descending order
        stock_date = datetime.strptime(d, "%Y-%m-%d")
        if stock_date >= cutoff_date:  # Only keep last 90 days
            stock_info = data["Time Series (Daily)"][d]
            stock_info["date"] = d  # Add date field
            results.append(stock_info)
    return results

@task
def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
          SYMBOL VARCHAR NOT NULL,
          DT DATE NOT NULL,
          SOPEN NUMBER(38,0),
          SCLOSE NUMBER(38,0),
          SHIGH NUMBER(38,0),
          SLOW NUMBER(38,0),
          SVOLUME NUMBER(38,0),
          PRIMARY KEY (SYMBOL, DT)
        );""")
        con.execute(f"DELETE FROM {target_table}")

        for i in records:
            sql = f"""
            INSERT INTO {target_table} (SYMBOL, DT, SOPEN, SCLOSE, SHIGH, SLOW, SVOLUME)
            VALUES (
                'TSLA',
                '{i["date"]}',
                {i["1. open"]},
                {i["4. close"]},
                {i["2. high"]},
                {i["3. low"]},
                {i["5. volume"]}
            )"""
            print(f"sql = {sql}")
            con.execute(sql)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'CountryCaptial',
    start_date = datetime(2025,2,28),
    catchup=False,
    tags=['ETL'],
    schedule = '30 8 * * *'
) as dag:
    target_table = "dev.raw.stock"
    cur = return_snowflake_conn()
    symbol = "TSLA"
    data = extract(symbol)
    load(cur, data, target_table)
