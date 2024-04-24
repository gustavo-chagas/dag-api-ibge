from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator 
from airflow.operators.python import PythonOperator 
import pandas as pd 
from unidecode import unidecode
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 

def transform_data(task_instance):
    info = task_instance.xcom_pull(task_ids='extract_data')
    list_info = info[0]['resultados'][0]['series']
    list_names = []
    for i in range(len(list_info)):
        list_names.append(list_info[i]['localidade']['nome'])

    list_frames = pd.DataFrame()
    for i in range(len(list_info)):
        if i ==0:
            v = pd.DataFrame(list_info[i]['serie'],index=[i])
            v1 = pd.concat([list_frames,v])
        else:
            v = pd.DataFrame(list_info[i]['serie'],index=[i])
            v1 = pd.concat([v1,v])

    v1['UF'] = list_names
    ipca = v1.T.rename(columns=v1['UF']).drop('UF')
    ipca = ipca.reset_index().rename({'index':'date'},axis=1)
    ipca['date'] = ipca['date'].apply(lambda x: x[-2:]+'/'+x[0:4])
    ipca = pd.melt(ipca,id_vars='date',var_name='Location',value_name='IPCA')
    ipca['Location'] = ipca['Location'].apply(lambda x: unidecode(x))
    ipca['IPCA'] = ipca['IPCA'].apply(lambda x: x.replace('.',','))
    task_instance.xcom_push(key='transformed_data',value=ipca.to_csv(index=False,sep=';'))

default_args = {
    'owner': 'Gustavo',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 14),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(dag_id='DAG-IBGE',
        default_args=default_args,
        schedule_interval='@weekly', 
        catchup=False
    )

is_ibge_api_ready = HttpSensor(task_id='is_ibge_api_ready',
    http_conn_id='ibge_api',
    endpoint='/api/v3/agregados/7060/periodos/-48/variaveis/2265?localidades=N1[all]|N7[all]&classificacao=315[7169]',
    dag=dag
)

extract_data = SimpleHttpOperator(
    task_id = 'extract_data',
    http_conn_id='ibge_api',
    endpoint='/api/v3/agregados/7060/periodos/-48/variaveis/63?localidades=N1[all]|N7[all]&classificacao=315[7169]',
    method='GET',
    response_filter=lambda x: x.json(),
    log_response=True,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_data = S3CreateObjectOperator(
    task_id='load_data',
    aws_conn_id='s3_conn',
    s3_bucket='ibgeapiairflow',
    s3_key='IPCA.csv',
    data="{{ task_instance.xcom_pull(key='transformed_data') }}",
    replace=True,
    dag=dag
)

is_ibge_api_ready >> extract_data >> transform_data >> load_data