import json
from multiprocessing.sharedctypes import Value
import requests
from campuslibs.loggers.mongo import save_to_mongo
from decouple import config
import time

def handle_enrollment(data, config):
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')

    requeue_no = 1

    while(requeue_no):
        if config.get('auth_type', '') == 'basic':
            try:
                response = requests.request(
                    "POST",
                    config.get('enrollment_url', ''),
                    auth=(
                        config.get('username', ''),
                        config.get('password', '')
                    ),
                    headers=headers,
                    data=json.dumps(data)
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                requeue_no += 1
                if requeue_no > config('MAX_RETRY_QUEUE_COUNT'):
                    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
                    return {'message': str(err)}
            else:
                break
        else:
            try:
                response = requests.request(
                    "POST",
                    config.get('enrollment_url', ''),
                    headers=headers,
                    data=json.dumps(data)
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                requeue_no += 1
                if requeue_no > config('MAX_RETRY_QUEUE_COUNT'):
                    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
                    return {'message': str(err)}
            else:
                break
        time.sleep(config('RETRY_INTERVAL'))

    resp = {}
    try:
        resp = response.json()
    except ValueError:
        resp = {'message': 'invalid response received'}
        save_to_mongo(data={'erp': 'j1:response', 'data': {'message': 'invalid response received'}}, collection='erp_response')
        return resp
    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': resp}}, collection='erp_response')
    return resp
