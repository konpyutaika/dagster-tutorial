from dagster import asset, AssetIn
import psycopg2
import json
import pandas.io.sql as psql
from typing import List, Tuple



@asset(io_manager_key="stream_string_io_manager")
def dagrun():
    """
    requeter dagrun airflow from metadata db & store on S3
    :return:
    """
    def _myiterator():
        query = """
        SELECT dag_id,execution_date,state,run_id,external_trigger,end_date,start_date,data_interval_end,run_type,last_scheduling_decision,queued_at
        from airflow.dag_run 
        limit 5
        """
        with psycopg2.connect("xxxxxx") as conn:
            for record in psql.read_sql_query(query, conn, chunksize=10000):
                dataframe = record.fillna("").astype(str).replace(r"\.0$", "", regex=True).replace({"NaT": None})
                list_records = dataframe.to_dict(orient="records")
                stringify = map(lambda r: json.dumps(r, ensure_ascii=False), list_records)
                yield from stringify
    return _myiterator


@asset(ins={"dagrun":AssetIn(key="dagrun",input_manager_key="s3_path_io_manager")})
def redshift_table(dagrun):
    print(dagrun)