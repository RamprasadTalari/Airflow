from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

#Different operators
#python operator
#bashoperator
#postgres operator
#define the arguments, we can add many more arguments here,but for ex.below defined for understanding

default_dag_args = {
    'start_date':datetime(2023,3,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retrydelay': timedelta(minutes=5),
    'project_id':1
}

# defining DAG

with DAG("First_DAG",schedule_interval = None, default_args = default_dag_args) as dag:

    # define tasks of DAG
    task_0 = BashOperator(task_id = 'bash_task',bash_command = "echo 'command executed from bash operator is proved' ")
    task_1 = BashOperator(task_id = 'bash_task_move_data',bash_command = "cp /Users/HP PC/Desktop/mynotes_ruff_usage/fol_1/testtomove.txt  /Users/HP PC/Desktop/mynotes_ruff_usage/fol_2")
    task_3 = BashOperator(task_id = 'Delete_file',bash_command = 'rm /Users/HP PC/Desktop/mynotes_ruff_usage/fol_1/testtomove.txt,)

    #define dependencies
    task_0 >> task_1 >> task_3