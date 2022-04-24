import json
from multiprocessing.sharedctypes import Value
import requests
from campuslibs.loggers.mongo import save_to_mongo

def handle_enrollment(data, config):
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')

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
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            return {'message': str(err)}
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
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            return {'message': str(err)}
    resp = {}
    try:
        resp = response.json()
    except ValueError:
        resp = {'message': 'invalid response received'}
        save_to_mongo(data={'erp': 'j1:response', 'data': {'message': 'invalid response received'}}, collection='erp_response')
        return resp
    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': resp}}, collection='erp_response')
    return resp
