from campuslibs.loggers.mongo import save_to_mongo
import requests

def handle_j1_enrollment(data):
    url = 'http://PDSVC-UNITY.JENZABARCLOUD.COM:9090/ws/rest/campus/api/enrollment/create'

    print('-------------ERP Payload---------------')
    print(data)
    print('---------------------------------------')

    response = requests.post(url, data=data)
    response_data = response.json()
    print('-------------ERP Response--------------')
    print(response_data)
    print('---------------------------------------')
    save_to_mongo(data={'erp': 'j1', 'data': response_data}, collection='erp_response')
