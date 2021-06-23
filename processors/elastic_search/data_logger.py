import requests
from decouple import config


def upload_log(data):
    es_host = config('ES_HOST')
    es_port = config('ES_PORT')
    method = 'POST'
    url = f'http://{es_host}:{es_port}/activity-log-student/_doc/'
    resp = requests.request(method, url, json=data)
    return resp.status_code
