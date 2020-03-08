from  __future__ import print_function

import time
from builtins import range
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner' : 'airflow',
    'start_date' : airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id = 'example_python_operator',
    default_args=args,
    schedule_interval=None,
)

# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printerd in the logs'

runt_this = PythonOperator(
    task_id = 'print_the_context',
    provide_context = True,
    python_callable = provide_context,
    dag = dag,
)

# [END howto_operator_python]

# [START howto_operator_python_kwargs]
def my_sleeping_function(ramdon_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)

# Generate 10 sleeping tasks, sleeping from 0 to 4 seconds respectively
for i in range(5):
    task = PythonOperator(
        task_id = 'sleep_for_' + str(i),
        python_callable = my_sleeping_function,
        op_kwargs = {'random_base' : float(i) / 10},
        dag = dag,
    )

runt_this >> task
# [END howto_operator_python_kwargs]