
# [START packages composer]

from airflow import DAG
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator

# packages provides
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
#https://airflow.apache.org/docs/stable/_api/airflow/contrib/operators/gcs_to_bq/index.html
#https://airflow.apache.org/docs/stable/_modules/airflow/contrib/example_dags/example_gcs_to_bq_operator.html
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils import trigger_rule

# [END composer]


#[START packages GCP]
from google.cloud import storage
#[END packages GCP]



#[START utils]
from datetime import datetime, timedelta
import pysftp
import json
#[END utils]

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


#############################################################################################

#############################################################################################



# Instancia de variables

params           = json.loads(Variable.get("PARAMS_TEST_FALABELLA"))
params_bq_schema = json.loads(Variable.get("PARAMS_BQ_SCHEMA_FIELDS"))
params_sftp      = json.loads(Variable.get("PARAMS_SFTP"))
query_sql        = Variable.get("QUERY_SQL")

schedule = params['CRON']
bq_recent_questions_table_id = '{}:{}.TEMP_TABLE'.format(params['GCP_PROJECT_ID'], params['BQ_DATASET_ID'])
list_gcs_to_bq = []



dag_nombre = 'dag_ejemplo'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'trigger_rule': 'all_done',
    'catchup_by_default': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2020, 5, 4, 10, 0)
}



dag = DAG(dag_nombre, default_args=default_dag_args, schedule_interval=schedule )

start = DummyOperator(task_id='start', dag=dag)



# Se leen los nombres de los archivos ( en este caso adhoc son 3), se iteran y se almacenan en BigQuery
for i, table_name in enumerate(params_bq_schema):
    gcs_to_bq = GoogleCloudStorageToBigQueryOperator( 
                        task_id = 'gcs_to_bq-{}'.format(table_name),
                        bucket  = params['BUCKET_ID'],
                        allow_quoted_newlines  = True, #Permite leer double quota ""
                        skip_leading_rows = 1, # Para que no lea el header
                        source_objects = ['data/{}.csv'.format(table_name)],
                        destination_project_dataset_table = '{}:{}.{}'.format(params['GCP_PROJECT_ID'],params['BQ_DATASET_ID'],table_name),
                        schema_fields =  params_bq_schema[table_name],
                        write_disposition = 'WRITE_TRUNCATE',
                        dag = dag
                )
    list_gcs_to_bq.append(gcs_to_bq)



# Se lee la querie de las variables de airflow para obtener el dataset final agrupado y almacenar en BigQuery
execute_bq_sql = BigQueryOperator(
                        task_id='execute_bq_sql',
                        sql= query_sql,
                        use_legacy_sql=False,
                        destination_dataset_table=bq_recent_questions_table_id,
                        create_disposition='CREATE_IF_NEEDED',
                        write_disposition='WRITE_TRUNCATE',
                        dag = dag
                )

# Se exporta el resultado de la tabla temporal a GCS

export_data_groupby = BigQueryToCloudStorageOperator(
                        task_id='export_table_temp_to_gcs',
                        source_project_dataset_table= bq_recent_questions_table_id,
                        destination_cloud_storage_uris='gs://{}/data/archivo_final_agrupado.csv'.format(params['BUCKET_ID']),
                        export_format='CSV',
                        dag = dag
                )

# La tabla creada con el dataset anterior se elimina ya que es temporal
delete_bq_dataset = BashOperator(
                    task_id='delete_table_temp',
                    bash_command='bq rm -f %s' % bq_recent_questions_table_id,
                    trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
                    dag = dag

                )

# Se suben los archivos de la agrupacion final, en conjunto con los archivos originales

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
                                     dag = dag
                        )

end   = DummyOperator(task_id='end', dag=dag, trigger_rule='all_done')


#Creación de grafo

start >> (list_gcs_to_bq) >> execute_bq_sql >> export_data_groupby >> delete_bq_dataset>> upload_files_to_sftp >> end