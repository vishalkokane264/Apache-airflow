from builtins import range
from datetime import timedelta,datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

import requests
import config as c
import os
import mysql.connector

args = {
    'owner': 'vishal@airflow',
    'start_date': datetime(2019,7,30,7,54),
}

dag = DAG(
    dag_id='Dag2',
    default_args=args,
    schedule_interval='54 7 * * *', 
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
    dbconn('Weather',dataset.columns.values,dataset)


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

def pdata():
	print("Hello")

run_this_last = PythonOperator(
    task_id='Csv_to_mysql',
    provide_context=True,
    python_callable=getdatafromcsv,
    dag=dag,
)

run_this = PythonOperator(
    task_id='Api_to_csv',
    provide_context=True,
    python_callable=getdata,
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last

