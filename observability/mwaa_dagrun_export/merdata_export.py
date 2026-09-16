from airflow.decorators import dag, task
from airflow.models import DagRun, Variable
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import timedelta, datetime
from utilities.logger import create_logger
from utilities.slack_operator import task_fail_slack_alert, task_success_slack_alert
import logging

# Define default arguments
default_args = {
    'owner': 'Data Warehouse Team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=90),
    'email': ["dmytrovaliaiev@gmail.com"],
    'on_failure_callback': task_fail_slack_alert,
    'on_success_callback': task_success_slack_alert,
}
# Setting a variable to fetch the last 5 days of logs only
MAX_AGE_IN_DAYS = 5

# Initialize logger
logger = create_logger(name="airflow_dagrun_export_logger", level=logging.INFO)


def _write_to_redshift(df: pd.DataFrame, table_name: str) -> None:
    """
    Writes a Pandas DataFrame to an Amazon Redshift table, overwriting rows for the
    DataFrame's maximum execution date if they exist, and appending new rows.

    This function:
    1. Truncates string values exceeding 256 characters.
    2. Converts the 'execution_date' column to a datetime format.
    3. Checks if the target table exists in Redshift; if not, it creates the table.
    4. Deletes rows in the Redshift table that match the maximum 'execution_date' in the DataFrame,
       using DATE_TRUNC to compare only the date parts.
    5. Inserts new rows from the DataFrame into the Redshift table.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be written to Redshift.
        table_name (str): The name of the target Redshift table (schema.table).

    Raises:
        psycopg2.DatabaseError: If a database error occurs during execution.
        Exception: For any other unexpected errors during execution.
    """
    try:
        logger.info(f"Starting to write data to Redshift table {table_name}. Number of rows: {len(df)}")

        df = df.applymap(lambda x: x if not isinstance(x, str) or len(x) <= 256 else x[:256])
        df['execution_date'] = pd.to_datetime(df['execution_date'])
        max_execution_date_in_df = df['execution_date'].max()

        # Filter the DataFrame to only include rows where the execution_date matches the max date
        filtered_df = df[df['execution_date'].dt.floor('D') == max_execution_date_in_df.floor('D')]

        # Prevent from collapsing if the execution did not work
        if filtered_df.empty:
            logger.warning(f"No data to insert for execution_date = {max_execution_date_in_df}.")
            return

        with psycopg2.connect(host=Variable.get('REDSHIFT_HOST'),
                              database=Variable.get('REDSHIFT_DBNAME'),
                              user=Variable.get('REDSHIFT_USER'),
                              password=Variable.get('REDSHIFT_PASSWORD'),
                              port=5439) as conn:
            with conn.cursor() as cur:
                logger.info(f"Checking if table {table_name} exists in Redshift.")
                cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                            (table_name.split('.')[-1],))
                table_exists = cur.fetchone()[0]

                if not table_exists:
                    logger.info(f"Table {table_name} does not exist. Creating it now.")
                    column_definitions = generate_column_definitions(df)
                    create_table_query = f"CREATE TABLE {table_name} ({column_definitions})"
                    cur.execute(create_table_query)
                else:
                    # Delete rows in Redshift where DATE_TRUNC('day', execution_date) matches max_execution_date_in_df
                    logger.info(
                        f"Deleting existing data in {table_name} for execution_date = {max_execution_date_in_df}.")
                    cur.execute(f"""
                        DELETE FROM {table_name}
                        WHERE DATE_TRUNC('day', execution_date) = DATE_TRUNC('day', %s)
                    """, (max_execution_date_in_df,))

                # Insert only the filtered data into Redshift
                logger.info(f"Inserting new data into {table_name} for execution_date = {max_execution_date_in_df}.")
                insert_query = f"INSERT INTO {table_name} ({', '.join(filtered_df.columns)}) VALUES ({', '.join(['%s'] * len(filtered_df.columns))})"
                execute_batch(cur, insert_query, filtered_df.values)

                conn.commit()

        logger.info(f"Successfully wrote {len(filtered_df)} rows to {table_name}.")

    except psycopg2.OperationalError as conn_err:
        logger.error(f"Database connection error: {conn_err}")
        raise
    except psycopg2.DatabaseError as db_err:
        logger.error(f"Database error: {db_err}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def _fetch_dagrun_records(session, max_age_in_days: int) -> list:
    """
    Fetches DagRun records from the Airflow database within the past `max_age_in_days`.

    Args:
        session: The database session used for querying the DagRun data.
        max_age_in_days (int): The number of days in the past to filter the DagRun records.

    Returns:
        list: A list of DagRun records.
    """
    query = session.query(DagRun).filter(DagRun.execution_date >= days_ago(max_age_in_days))
    return query.all()


def _convert_to_dataframe(rows: list) -> pd.DataFrame:
    """
    Converts a list of DagRun records to a Pandas DataFrame.

    Args:
        rows (list): List of DagRun records.

    Returns:
        pd.DataFrame: DataFrame containing the DagRun data with all columns in lowercase and values cast to strings.
    """
    if len(rows) == 0:
        logger.warning("No DagRun metadata found for the given time period.")
        return pd.DataFrame()

    lst = [vars(row) for row in rows]
    dagrun_df = pd.DataFrame(lst)
    dagrun_df.columns = map(str.lower, dagrun_df.columns)
    dagrun_df = dagrun_df.astype(str, errors='ignore')  # Convert all to string
    return dagrun_df

def generate_column_definitions(df: pd.DataFrame) -> str:
    """
    Generates Redshift-compatible column definitions based on the Pandas DataFrame's data types.

    Args:
        df (pd.DataFrame): DataFrame for which column definitions will be generated.

    Returns:
        str: A string of column definitions for a Redshift CREATE TABLE statement.
    """
    # Define the mapping between Pandas types and Redshift SQL types
    dtype_mapping = {
        'int64': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
        'object': 'VARCHAR(256)'  # Default for strings
    }

    # Generate column definitions based on the DataFrame's dtypes
    column_definitions = []
    for col, dtype in df.dtypes.items():
        # Dynamically determine the type, defaulting to VARCHAR(256) if not found in the mapping
        redshift_type = dtype_mapping.get(str(dtype), 'VARCHAR(256)')
        column_definitions.append(f"{col} {redshift_type}")

    return ', '.join(column_definitions)


def _export_dagrun_data() -> pd.DataFrame:
    """
    Fetch metadata for DagRuns from the past MAX_AGE_IN_DAYS and return as a Pandas DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing the DagRun metadata, or an empty DataFrame if no records are found.
    """
    logger.info(f"Starting extraction of DagRun metadata for the last {MAX_AGE_IN_DAYS} days.")

    with create_session() as session:
        rows = _fetch_dagrun_records(session, MAX_AGE_IN_DAYS)
        dagrun_df = _convert_to_dataframe(rows)

    logger.info(f"Extracted {len(dagrun_df)} rows of DagRun metadata.")
    return dagrun_df


@dag(
    description="This writes dargun information from Airflow metabase into Redshift",
    default_args=default_args,
    schedule="0 21 * * *",  # 3 PM CST
    start_date=datetime(2024, 9, 1),
    dagrun_timeout=timedelta(minutes=45),
    catchup=False,
    max_active_runs=1
)
def airflow_dagrun_export() -> None:
    @task(task_id="export_and_write_dagrun_metadata")
    def export_and_write_dagrun_metadata() -> None:
        """
        Single task to fetch DagRun metadata and write it to Redshift.
        """
        # Fetch DagRun metadata
        dagrun_df = _export_dagrun_data()

        used_columns = ["run_id",
                        "dag_id",
                        "execution_date",
                        "end_date",
                        "_state",
                        "external_trigger",
                        "run_type"]

        # unused_columns = ["data_interval_start",
        #                 "start_date",
        #                  "data_interval_end",
        #                  "last_scheduling_decision",
        #                  "dag_hash",
        #                  "creating_job_id",
        #                  "log_template_id"]

        # Subset to the columns of interest
        available_columns = [col for col in used_columns if col in dagrun_df.columns]
        dagrun_df = dagrun_df[available_columns]

        missing_columns = [col for col in used_columns if col not in dagrun_df.columns]
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns} in DagRun DataFrame.")

        # Write to Redshift if data is available
        if not dagrun_df.empty:
            table_name = "elementary.airflow_dagrun_metadata"  # Change to your desired table name
            _write_to_redshift(dagrun_df, table_name)
        else:
            logger.warning("No data to write to Redshift.")

    export_and_write_dagrun_metadata()


airflow_dagrun_export()
