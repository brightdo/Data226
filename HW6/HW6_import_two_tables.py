from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector




@task
def prepare_tables():
    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='DRAGON_QUERY_WH',
        database='USER_DB_DRAGON'
    )
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS USER_DB_DRAGON.RAW.user_session_channel (
        userId int not NULL,
        sessionId varchar(32) primary key,
        channel varchar(32) default 'direct'  
        );""")


        cur.execute(f"""CREATE TABLE IF NOT EXISTS USER_DB_DRAGON.RAW.SESSION_TIMESTAMP (
        sessionId varchar(32) primary key,
        ts timestamp  
        );""")

        cur.execute(f"""
        CREATE OR REPLACE STAGE USER_DB_DRAGON.raw.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
            """)
        
        cur.execute(f"""
        COPY INTO USER_DB_DRAGON.raw.user_session_channel
        FROM @USER_DB_DRAGON.raw.blob_stage/user_session_channel.csv;
        """)

        cur.execute(f"""
        COPY INTO USER_DB_DRAGON.raw.session_timestamp
        FROM @USER_DB_DRAGON.raw.blob_stage/session_timestamp.csv;
        """)

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'import_two_tables',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    prepare_tables()