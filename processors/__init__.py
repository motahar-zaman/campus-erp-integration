import json
from status_logger import save_status_to_mongo
from .mindedge.enrollment import enroll
from .hubspot.data_service import send_user_data


def mindedge_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    status_data = {'comment': 'received', 'data': data}
    save_status_to_mongo(status_data)
    enroll(data)


def hubspot_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_user_data(data)
