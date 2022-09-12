import json
from multiprocessing.sharedctypes import Value
import requests
from campuslibs.loggers.mongo import save_to_mongo
from decouple import config
import time

def handle_enrollment(data, configuration, payload, ch):
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')

    requeue_no = 1
    exchange_dead_letter = 'dlx'
    routing_key = 'enrollment.enroll'

    while(requeue_no):
        if configuration.get('auth_type', '') == 'basic':
            try:
                response = requests.request(
                    "POST",
                    configuration.get('enrollment_url', ''),
                    auth=(
                        configuration.get('username', ''),
                        configuration.get('password', '')
                    ),
                    headers=headers,
                    data=json.dumps(data)
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                requeue_no += 1
                if requeue_no > int(config('TASK_MAX_RETRY_COUNT')):
                    if payload['retry_queue_count'] < int(config('MAX_DEADLETTER_QUEUE_COUNT')):
                        payload['retry_queue_count'] += 1
                        ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))
                    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
                    return {'message': str(err)}
            else:
                break
        else:
            try:
                response = requests.request(
                    "POST",
                    configuration.get('enrollment_url', ''),
                    headers=headers,
                    data=json.dumps(data)
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                requeue_no += 1
                if requeue_no > int(config('TASK_MAX_RETRY_COUNT')):
                    if payload['retry_queue_count'] < int(config('MAX_DEADLETTER_QUEUE_COUNT')):
                        payload['retry_queue_count'] += 1
                        ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))
                    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
                    return {'message': str(err)}

            else:
                break
        time.sleep(requeue_no * int(config('RETRY_INTERVAL')))

    resp = {}
    try:
        resp = response.json()
    except ValueError:
        resp = {'message': 'invalid response received'}
        save_to_mongo(data={'erp': 'j1:response', 'data': {'message': 'invalid response received'}}, collection='erp_response')
        return resp
    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': resp}}, collection='erp_response')
    return resp
