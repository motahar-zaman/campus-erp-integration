import json
import requests
from campuslibs.loggers.mongo import save_to_mongo

def handle_j1_enrollment(data):
    url = 'http://PDSVC-UNITY.JENZABARCLOUD.COM:9090/ws/rest/campus/api/enrollment/create'
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')
    response = requests.request("POST", url, headers=headers, data=json.dumps(data))
    save_to_mongo(data={'erp': 'j1:response', 'data': response.json()}, collection='erp_response')
