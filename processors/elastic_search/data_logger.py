import requests
from decouple import config


def upload_log(data):
    baseURL = config('ES_BASE_URL')
    method = 'POST'
    url = f'{baseURL}/activity-log-student/_doc/'
    resp = requests.request(method, url, json=data)
    return resp.status_code
