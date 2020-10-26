import boto3
import botocore
import os.path
import shutil
import json
from datetime import datetime, timedelta

def create_local_pipeline_dir():
    if os.path.exists(f'/usr/local/amazon/pipeline'):
        shutil.rmtree(f'/usr/local/amazon/pipeline') 
    os.makedirs(f'/usr/local/amazon/output')
    os.makedirs(f'/usr/local/amazon/temp_data')



def get_load_type():

    if os.path.exists(f'/usr/local/pipeline/state_previous.json'):
        return f'incremental_load'
    else:
        return f'initial_load'


def download_file(bucket, key, destination):
    s3 = boto3.resource('s3')

    if os.path.exists(destination):
        os.remove(destination)

    try:
        mykey = s3.Object(bucket,key).get()

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == 'NoSuchKey':
            # The object does not exist.
            pass
        else:
            # Something else has gone wrong.
            raise
    else:
        result = mykey['Body'].read().decode('utf-8')
        f = open(destination, 'w+')
        f.write(result)
        f.close()


def upload_file(source, bucket, key):
    s3 = boto3.resource('s3')

    s3.meta.client.upload_file(source, bucket, key)

def upload_dir(dir, bucket, tap):
    for filename in os.listdir(dir):
        source = f'{dir}/{filename}'
        key = f'output/{filename}'
        upload_file(source, bucket, key)

def upload_dir_split(dir, bucket, tap):
    for filename in os.listdir(dir):
        source = f'{dir}/{filename}'
        output_dir = filename[0:filename.index('-')]
        key = f'{tap}/output/{output_dir}/{filename}'
        upload_file(source, bucket, key)

def change_tap_start_date(tap_config):
    #Open the locally saved tap_config.json file
    with open(tap_config) as f:
        config = json.load(f)
        new_start_date = datetime.utcnow() - timedelta(days=30)
        #write in the new_start_date
        config['start_date'] = new_start_date.strftime('%Y-%m-%dT00:00:00Z')
    #resave the file locally 
    with open(tap_config, 'w') as f:
        json.dump(config,f, indent=2)
