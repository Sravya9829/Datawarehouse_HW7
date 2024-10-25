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
def create_session_summary_table():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Create the session_summary table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS analysis.session_summary (
            sessionId varchar(32) PRIMARY KEY,
            userId int,
            channel varchar(32),
            ts timestamp
        );
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("session_summary table created successfully.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred: {e}")
        raise e

@task
def join_tables_and_insert():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Insert data into session_summary by joining the two tables
        cur.execute("""
        INSERT INTO analysis.session_summary (sessionId, userId, channel, ts)
        SELECT 
            usc.sessionId, 
            usc.userId, 
            usc.channel, 
            st.ts
        FROM dev.raw_data.user_session_channel usc
        JOIN dev.raw_data.session_timestamp st
        ON usc.sessionId = st.sessionId;
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("Data inserted into session_summary table.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred: {e}")
        raise e

@task
def check_and_remove_duplicates():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")  # Start transaction

        # Check for and remove duplicates based on sessionId
        cur.execute("""
        DELETE FROM analysis.session_summary
        WHERE sessionId IN (
            SELECT sessionId
            FROM (
                SELECT sessionId, COUNT(*) 
                FROM analysis.session_summary 
                GROUP BY sessionId 
                HAVING COUNT(*) > 1
            )
        );
        """)

        cur.execute("COMMIT;")  # Commit transaction
        print("Duplicate records removed from session_summary table.")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction in case of failure
        print(f"Error occurred while removing duplicates: {e}")
        raise e

# Define the DAG
with DAG(
    dag_id='ELTDAG',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='@daily'  # Adjust schedule as needed
) as dag:

    # Task: Create session_summary table
    create_session_summary_task = create_session_summary_table()

    # Task: Perform JOIN and insert data into session_summary
    join_tables_and_insert_task = join_tables_and_insert()

    # Task: Check and remove duplicate records
    check_and_remove_duplicates_task = check_and_remove_duplicates()

    # Define task dependencies
    create_session_summary_task >> join_tables_and_insert_task >> check_and_remove_duplicates_task
