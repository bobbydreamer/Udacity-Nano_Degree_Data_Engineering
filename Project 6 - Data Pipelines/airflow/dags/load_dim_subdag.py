#Instructions
#In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
#1 - Consolidate HasRowsOperator into the SubDag
#2 - Reorder the tasks to take advantage of the SubDag Operators

import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import HasRowsOperator
#from airflow.operators.udacity_plugin import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (LoadDimensionOperator)

import logging

#import sql


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def load_dim_table_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        create_sql_stmt,
        select_stmt,
        append_rows,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    #create_dimtable_task = DummyOperator(task_id="create_{}_table".format(table),  dag=dag)
    create_dimtable_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    #insert_to_table = DummyOperator(task_id="insert_into_{}".format(table),  dag=dag)
    #insert_to_table = PostgresOperator(
    #    task_id=f"insert_into_{table}",
    #    dag=dag,
    #    postgres_conn_id=redshift_conn_id,
    #    sql=insert_sql.format(
    #        table,
    #        select_stmt
    #    )
    #)
    insert_to_table = LoadDimensionOperator(
        task_id=f"insert_into_{table}",
        dag=dag,
        redshift_conn_id="redshift",
        table=table,
        sql_source=select_stmt,
        append_rows=append_rows
    )
    
    #check_task = DummyOperator(task_id="check_{}_data".format(table),  dag=dag)
    check_task = HasRowsOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table
    )

    create_dimtable_task >> insert_to_table
    insert_to_table >> check_task
    
    return dag
