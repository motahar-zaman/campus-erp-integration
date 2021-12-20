from services.mindedge import MindEdgeService
from campuslibs.loggers.mongo import save_to_mongo
import requests

def handle_j1_enrollment(data):
    save_to_mongo(data={'comment': 'input data', 'data': data})
    url = 'http://PDSVC-UNITY.JENZABARCLOUD.COM:9090/ws/rest/campus/api/enrollment/create'

    response = requests.post(url, params=data)
    response_data = response.json()
    save_to_mongo(data={'comment': 'response data', 'data': response_data}, collection='handle_j1_enrollment')
