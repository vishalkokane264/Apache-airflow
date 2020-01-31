import requests
import config as c
import os
import mysql.connector
import airflow
import time 

from builtins import range
from datetime import timedelta,datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine


args = {
    'owner': 'vishal@airflow',
    'retries':1,
    'start_date': datetime(2019,7,29,18,39),
}

dag = DAG(
    dag_id='Dag3.1',
    default_args=args,
    schedule_interval='1 * * * *', 
    dagrun_timeout=timedelta(minutes=60),
)
def dbconn(tblname,dblist,dataset):
    print(dblist)
    engine = create_engine("mysql+pymysql://root:"+'Vishal@12'+"@localhost/empl")
    connection=engine.connect()
    table_name=tblname
    createsqltable = """CREATE TABLE IF NOT EXISTS """ + table_name + " (" + " VARCHAR(250),".join(dblist) + " VARCHAR(250))"
    connection.execute(createsqltable)
    dataset.to_sql(name=tblname, con=connection, if_exists = 'replace', index=False)
    print("successfully data inserted into table")

def getdatafromcsv(ds,**kwargs):
    import pandas as pd
    dataset=pd.read_csv('/home/vishal/airflow/dags/mydata1.csv')
    print(dataset.columns.values)
    dbconn('Weather1',dataset.columns.values,dataset)


def getdata(ds,**kwargs):
    result=requests.get("https://api.data.gov.in/resource/201b66f2-7fda-40b8-b613-ffb7789c4341?api-key=579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b&format=csv&offset=0&limit=10")
#    result=requests.get("https://api.data.gov.in/resource/44741368-77ff-4a20-8423-cd6a091d6782?api-key=579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b&format=csv&offset=0&limit=10")
    if result.status_code==200:
        with open('/home/vishal/airflow/dags/mydata1.csv','w') as f:
            for line in result.text:
                f.write(line)
        f.close()
    else:
        print("Error in api call")

def function1(ds,**kwargs):
    for i in range(0,20):
        print(i,"]Hello from function1")
    print('exit from function1')

def Arithmetic_function1(ds,**kwargs):
	print("Arithmetic function1")

def function2(ds,**kwargs):
    print("In function2")

def function3(ds,**kwargs):
    print("I'm exiting now...")

#
task1= PythonOperator(
    task_id='t_function1',
    provide_context=True,
    python_callable=function1,
    dag=dag,
)

task2= PythonOperator(
    task_id='t_getdatafromapitocsv',
    provide_context=True,
    python_callable=getdata,
    dag=dag,
)

task3= PythonOperator(
    task_id='t_getdatafromcsvtoframes',
    provide_context=True,
    python_callable=getdatafromcsv,
    dag=dag,
)

task4= PythonOperator(
    task_id='t_function2',
    provide_context=True,
    python_callable=Arithmetic_function1,
    dag=dag,
)

task5= PythonOperator(
    task_id='t_Arithmetic_function1',
    provide_context=True,
    python_callable=Arithmetic_function1,
    dag=dag,
)

task6= PythonOperator(
    task_id='t_function3',
    provide_context=True,
    python_callable=function3,
    dag=dag,
)

# [END howto_operator_bash]

# Dependencies

task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
task5.set_upstream(task4)
task6.set_upstream(task5)
