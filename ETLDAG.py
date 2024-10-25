from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_stage_and_tables():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Create Stage
        cur.execute("""
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        URL = 's3://s3-geospatial/readonly/'
        FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)

        # Create user_session_channel table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int NOT NULL,
            sessionId varchar(32) PRIMARY KEY,
            channel varchar(32) DEFAULT 'direct'
        );
        """)

        # Create session_timestamp table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) PRIMARY KEY,
            ts timestamp
        );
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("Stage and tables created successfully.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred: {e}")
        raise e

@task
def load_user_session_channel():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Load data into user_session_channel table
        cur.execute("""
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv
        FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("Data loaded into user_session_channel table.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred during load: {e}")
        raise e

@task
def load_session_timestamp():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Load data into session_timestamp table
        cur.execute("""
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv
        FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("Data loaded into session_timestamp table.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred during load: {e}")
        raise e

# Define DAG
with DAG(
    dag_id='ETLDAG',
    start_date=datetime(2024, 10, 20),
    catchup=False,
    tags=['ETL'],
    schedule='@daily'  # Set to your preferred schedule
) as dag:

    # Task: Create stage and tables
    create_stage_and_tables_task = create_stage_and_tables()

    # Task: Load data into user_session_channel table
    load_user_session_channel_task = load_user_session_channel()

    # Task: Load data into session_timestamp table
    load_session_timestamp_task = load_session_timestamp()

    # Define task dependencies
    create_stage_and_tables_task >> [load_user_session_channel_task, load_session_timestamp_task]
