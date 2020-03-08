from __future__ import print_function

import time
from builtins import range
import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner' : 'airflow',
    'start_date' : airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id = 'conditional_dag',
    default_args= args,
    schedule_interval= '0 0 * * *',
)

def decide_flow(**context):
    if (context['execution_date'] < datetime.datetime(2018,1,1)):
        return "old_way"
    return "new_way"

process_sales_old = DummyOperator(
    task_id = 'old_way',
    dag = dag
)

process_sales_new = DummyOperator(
    task_id = "new_way",
    dag = dag
)

stage_sales = DummyOperator(
    task_id = 'stage_sales',
    dag = dag
)

decide = BranchPythonOperator(
    python_callable = decide_flow,
    provide_context = True,
    task_id = "select_processing_method",
    dag = dag
)

process_aggregate = DummyOperator(
    trigger_rule="all_done",
    task_id = "process_aggregate",
    dag = dag 
)

stage_sales >> decide
decide >> process_sales_old
decide >> process_sales_new
process_sales_old >> process_aggregate
process_sales_new >> process_aggregate
