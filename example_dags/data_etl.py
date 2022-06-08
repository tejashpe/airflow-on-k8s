import json
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import pendulum
import pandas as pd
import numpy as np
import boto3

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.models import Variable


# [END import_module]

# [START instantiate_dag]
with DAG(
    'custom_etl_withxcode',
    default_args={'retries': 2},
    description='ETL DAG with lineage',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['custom'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]
    def extract(**kwargs):
        import os
        import boto3
        from airflow.models import Variable
        import requests

        os.environ['HTTP_PROXY']=Variable.get('PROXY')
        os.environ['HTTPS_PROXY']=Variable.get('PROXY')
        os.environ['NO_PROXY']=Variable.get('NO_PROXY')
        print('Downloading started')
        url = Variable.get('DATA_SOURCE')

        # Downloading the file by sending the request to the URL
        req = requests.get(url)
        
        # Split URL to get the file name
        filename = url.split('/')[-1]
        
        # Writing the file to the local file system
        with open(filename,'wb') as output_file:
            output_file.write(req.content)
        
        import zipfile
        with zipfile.ZipFile(filename, 'r') as zip_ref: 
            zip_ref.extractall(filename+"_extract")
        print('Downloading Completed')

        s3_client = boto3.client('s3', endpoint_url=Variable.get('S3_ENDPOINT'), 
            aws_access_key_id= Variable.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'), verify=False)
        response = s3_client.upload_file(f'{filename}_extract/adult_data.csv', 'data', "data/{}".format('adult.csv'))
        print(response)

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        print("Transforming the data")
        import os
        rootdir = '.'
        for it in os.scandir(rootdir):
            if it.is_dir():
                print(it.path)

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        print("Load the transformed data to volume")
    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a CSV data on enterprise finance expense.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of expense data from xcom
    and computes the total expense.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> load_task