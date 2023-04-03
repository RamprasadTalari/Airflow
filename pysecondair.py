from airflow import DAG
from airflow.operators.python_operator import pythonOperator
from datetime import datetime,timedelta

# for ref : crontab notation https://crontab.guru/
# Process of crerating Dag with python operator
# First create a python function tha we wanted to call in Dag
# Call the python fuction using Dag with Python operator.

# create python logic or function whuich we wanted to call.
def python_first_function():
    present = datetime.datetime.present()
    print("Today's date and time is: ",present.strframe("%Y-%m-%d %H:%M:%S"))

# Create Dag to call the above pyhthon function
default_dag_args = {
    'start_date':datetime(2023,3,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retrydelay': timedelta(minutes=5),
    'project_id':1
}

with DAG("first_python_dag",schedule_interval = '@daily',catchup = False,default_args = default_dag_args) as dag_python:

    #define Dag tasks
    task_python = pythonOperator(task_id ="first_python_task" , python_callable = python_first_function)


#PythonOperator + insights about schedule_interval