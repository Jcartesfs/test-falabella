# Importar librerias de airflow
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators import bigquery_operator
# [END composer_bigquery]
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import bash_operator
from datetime import  date, time, timedelta,datetime
import calendar
from airflow.contrib.hooks.ssh_hook import SSHHook
import pysftp


from airflow.models import Variable

# Apis GCP
from google.cloud import storage

# Utils
from datetime import datetime, timedelta
import pytz
import json


#############################################################################################

#############################################################################################
'''
def get_data_from_storage(bucket_name, file_name= None):
    client_storage = storage.Client()
    bucket = client_storage.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(delimiter='/')) #prefix='/'
    
    file_name_csv = ''
    # Se busca el nombre del archivo
    for obj in blobs:
        if  'Rick' in blob.name:
             file_name_csv = blob.name
             break


    blob = bucket.get_blob(file_name_csv)
    data = blob.download_as_string()
    #data = data.decode('iso-8859-1').encode('utf8')
    data = data.decode("utf-8")

    return data


def generate_df(**kwargs):

    bucket_name = kwargs.get('templates_dict').get('bucket_name')
    data = get_data_from_storage(bucket_name)
    data_df = data
    columns = ['index', 'season', 'episode', 'episode_name', 'actor_name','line']

    data_df = [tuple(str(row).split(',')[:len(columns)]) for row in data.split('\n') ]


    df = pd.DataFrame(data_df, columns = columns)
    print('**************************************+')
    print(df.loc[(df['season'] == 1) & (df['episode'] == 1)])
    print('**************************************+')


    tz = pytz.timezone('America/Santiago')
    santiago_now = datetime.now(tz)
    fecha_csv = str(santiago_now.strftime('%Y%m%d'))

     #Se realiza proceso push
    kwargs['ti'].xcom_push(key='dataset_episode_daily', value= 'valores ejemplos')
    '''
#############################################################################################

#############################################################################################
def upload_file_sftp(**kwargs):

    host  = kwargs.get('templates_dict').get('hostname')
    username  = kwargs.get('templates_dict').get('username')
    password  = kwargs.get('templates_dict').get('password')
    bucket_name  = kwargs.get('templates_dict').get('bucket_name')


    client_storage = storage.Client()
    bucket = client_storage.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix='data')) #prefix='/'
   

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  

    for blob in blobs:
        file_name = blob.name.split('/')[1]
        if '.csv' in file_name:
            with pysftp.Connection(host, username, password = password, cnopts=cnopts) as sftp:
                blob.download_to_filename(file_name)
                sftp.put(file_name,file_name) #upload file to nodejs/
    return ''



params = json.loads(Variable.get("PARAMS_TEST_FALABELLA"))
params_bq_schema = json.loads(Variable.get("PARAMS_BQ_SCHEMA_FIELDS"))
params_sftp = json.loads(Variable.get("PARAMS_SFTP"))

query_sql = Variable.get("QUERY_SQL")

schedule = params['CRON']
bq_recent_questions_table_id = '{}:{}.TEMP_TABLE'.format(params['GCP_PROJECT_ID'], params['BQ_DATASET_ID'])



dag_nombre = 'test_sftp'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'trigger_rule': 'all_done',
    'catchup_by_default': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2020, 5, 7, 10, 0) #datetime.utcnow()
}



dag = DAG(dag_nombre,
          default_args=default_dag_args,
          schedule_interval=schedule
          )

start = DummyOperator(task_id='start', dag=dag)

upload_files_to_sftp = PythonOperator(task_id="upload_file_to_sftp",
                                      execution_timeout=timedelta(hours=1),
                                      python_callable=upload_file_sftp,
                                      provide_context=True,
                                      trigger_rule='all_success',
                                      templates_dict={
                                                    'hostname':    params_sftp['HOST'],
                                                    'username':    params_sftp['USERNAME'],
                                                    'password':    params_sftp['PWD'],
                                                    'port':        params_sftp['PORT'],
                                                    'bucket_name': params['BUCKET_ID']

                                                     },
                                     dag = dag)


end   = DummyOperator(task_id='end', dag=dag, trigger_rule='all_done')


start >> upload_files_to_sftp >> end
