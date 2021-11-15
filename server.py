from flask import Flask, request
from setup import init
from middleware import validate_authorization
import json
import requests
import os
import time
import re
import pandas as pd


app = Flask(__name__)

MAX_BATCH_SIZE, MAX_PARALLEL_BATCHES, MAX_RECORDS_PER_SECOND = 100, 1, 1

ENDPOINTS, OPERATION_MAP = ['endpoint1', 'endpoint2'], {'endpoint1': ['upsert', 'update'], 'endpoint2': ['update']}


@app.route('/')
def validate_token():
    if validate_authorization(request):
        return 'Welcome to microservice'
    else:
        return 'Not authenticated'

def list_objects():
    result_objects = []
    for object_api_name in ENDPOINTS:
        # Need to change as you want to display this object within Census
        result_objects.append({"object_api_name": object_api_name, "label": object.display_name})
    return result_objects

def get_fields(object_api_name):
    fields = []
    headers = {'Authorization': os.environ.get('SERVICE_AUTHORIZATION_TOKEN'), 'Content-Type': 'application/json'}
    data = requests.request("GET",os.environ.get('BASE_URL')+object_api_name, headers=headers)
    for field in data.fields:
        fields.append({'name': field.name, 'type': field.type})
    return fields

def organize_data(df, columns, keys):
    data = []
    # Add proper formatting for your API
    return data

def call_bulk_api(object_api_name, keys, df, columns):
    # Where you call the api
    results = []
    
    # Every API is different, but you should organize the data accordingly using this helper method
    data = organize_data(df, columns, keys)
    
    headers = {'Authorization': os.environ.get('SERVICE_AUTHORIZATION_TOKEN'), 'Content-Type': 'application/json'}
    data = requests.request("POST",os.environ.get('BASE_URL')+object_api_name, headers=headers)
    for row in data.results:
        results.append({'identifier': row['identifier'], 'success': row['success'], 'error_message': row['error_message']})
    return results


def list_fields(params):
    fields = []

    api_name = params['object']['object_api_name']
    fields = get_fields(api_name)
    for column in fields:
        type = str(column['type'])
        # Perform the neccesary splitting to get the right type, below is an example
        type_split = re.split(' |\(|\)|,', type)

        # This is not exhaustive, but should serve as a guide for proper type conversions
        census_type, census_array = None, False
        if type_split[0] == 'DECIMAL':
            census_type = 'integer' if type_split[3] == '0' else 'decimal'
        elif type_split[0] == 'INTEGER':
            census_type = 'integer'
        elif type_split[0] == 'ARRAY':
            census_type = 'string'
            census_array = True
        elif type_split[0] == 'FLOAT':
            census_type = 'float'
        elif type_split[0] == 'VARCHAR':
            census_type = 'string'
        elif type_split[0] == 'BOOLEAN':
            census_type = 'boolean'
        elif type_split[0] == 'DATE':
            census_type = 'date'
        elif type_split[0] in ('TIMESTAMP_NTZ', 'DATETIME', 'TIMESTAMP_TZ', 'TIMESTAMP'):
            census_type = 'date_time'
        else: 
            census_type = 'unknown'

        field = {
            "field_api_name": column['name'],
            "label": column['name'],
            "identifier": census_type in ['string', 'integer'] and census_array is False,
            "required": False,  # These need to be based on your implementation of your object
            "createable": True, # These need to be based on your implementation of your object
            "updateable": True, # These need to be based on your implementation of your object
            "type": census_type,
            "array": census_array
        }
        fields.append(field)

    return fields


def supported_operations(params):
    object = params['object']
    return OPERATION_MAP[object['object_api_name']]

def sync_batch(params, id):
    sync_results = []
    sync_operation, object_api_name = params['sync_plan']['operation'], params['sync_plan']['object']['object_api_name']
    schema = params['sync_plan']['schema']
    key, columns = None, []
    for col, val in schema.items():
        if val['active_identifier']: 
            key = col
        columns.append({'name': col, 'type': val['field']['type']})
    if sync_operation == 'upsert':
        df = pd.DataFrame(params['records'])
        keys = list(df[key])
        results = call_bulk_api(object_api_name, keys, df, columns)
        for row in results:
            id, success, error_message = row['identifier'], row['success'], row['error_message']
            if success:
                sync_results.append({
                    "identifier": id,
                    "success": True
                })
            else:
                sync_results.append({
                    "identifier": id,
                    "success": False,
                    "error_message": error_message
                })
        return sync_results
    elif sync_operation == 'update':
        # Do your processing here
        return sync_results
    return sync_results


@app.route('/census-custom-api', methods = ['POST'])
def run_method_router():
    try:
        # When configuring the Custom API in Census this should be specified as 
        # http://myurl.com/census-custom-api?census-api-key=S3CR3TT0K3N 
        # Thusly, the following line will return "S3CR3TT0K3N"
        auth_variable = request.args.get('census-api-key') 
        jsonrpc, method, id, params, validated = validate_authorization(request, auth_variable)
        if validated:
            if method == 'test_connection':
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': True}})
            elif method == 'list_objects':
                objects = list_objects()
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'objects': objects}})
            elif method == 'list_fields':
                fields = list_fields(params)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'fields': fields}})
            elif method == 'supported_operations':
                operations = supported_operations(params)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'operations': operations}})
            elif method == 'get_sync_speed':
                maximum_batch_size, maximum_parallel_batches, maximum_records_per_second = MAX_BATCH_SIZE, MAX_PARALLEL_BATCHES, MAX_RECORDS_PER_SECOND
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'maximum_batch_size': maximum_batch_size, 'maximum_parallel_batches': maximum_parallel_batches, 'maximum_records_per_second': maximum_records_per_second}})
            elif method == 'sync_batch':
                sync_results = sync_batch(params, id)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'record_results': sync_results}})
            else:
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': False, 'error_message': 'That method is not supported'}})
        else:
            return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': False, 'error_message': 'The API Key is invalid'}})
    except Exception as e:
        print('Error '+ e)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
