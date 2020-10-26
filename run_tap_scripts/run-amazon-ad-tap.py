#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 14:35:35 2020
@author: cairothompson
"""
######################## import modules ##############################################
from script.local_module import create_local_pipeline_dir, get_load_type, download_file, upload_file, upload_dir, change_tap_start_date
import os
######################### Set Local Variables ################################################
#Gets the AWS Batch stored env variable (there are 10 diifferent endpoint pipelines for eltoro)
tap = 'tap_amazon_advertising'
target = 'target-json'
local_path = '/usr/local/amazon/pipeline'

# s3 paths
bucket = 'tap-amazon-advertising'

########################## Functions #####################################################3
# create the local pipeline directory
create_local_pipeline_dir()

# download state_previous.json locally
download_file(bucket, 'pipeline/state_previous.json', local_path+'/state_previous.json')
# download catalog.json locally
download_file(bucket, 'pipeline/catalog.json',local_path+'/catalog.json')
# download tap_config.json locally
download_file(bucket,'pipeline/tap_config.json',local_path+'/tap_config.json')
# download target_config.json locally
download_file(bucket, 'pipeline/target_config.json',local_path+'/target_config.json')


#change the tap_config.json start date to 30 days ago
######change_tap_start_date(local_path+'/tap_config.json')
# load_type_branch
load_type = get_load_type()

if (load_type == 'incremental_load'):
    # incremental_load
    os.system('cd /usr/local/amazon/output && rm -f * && /usr/local/amazon/venvs/'+tap+'/bin/'+tap+' --config '+local_path+'/tap_config.json --catalog '+local_path+'/catalog.json --state '+local_path+'/state_previous.json | /usr/local/amazon/venvs/'+target+'/bin/'+target+' --config '+local_path+'/target_config.json > '+local_path+'/state.json')
else:
    # initial_load 
    os.system('cd /usr/local/amazon/output && rm -f * && /usr/local/amazon/venvs/'+tap+'/bin/'+tap+' --config '+local_path+'/tap_config.json --catalog '+local_path+'/catalog.json | /usr/local/amazon/venvs/'+target+'/bin/'+target+' --config '+local_path+'/target_config.json > '+local_path+'/state.json')

# upload the target (the response from the API calls) to S3
upload_dir('/usr/local/amazon/output',bucket,tap)
# save the state of the locations processed
os.system('tail -1 '+local_path+'/state.json > '+local_path+'/state.json.tmp && mv '+local_path+'/state.json.tmp '+local_path+'/state_previous.json') 
# upload state_previous.json
upload_file(local_path+'/state_previous.json','pipeline/state_previous.json')
#upload tap_config.json
#####upload_file(local_path+'/tap_config.json',bucket,'pipeline/tap_config.json')
