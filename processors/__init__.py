import json
from status_logger import save_status_to_mongo
from .mindedge.enrollment import enroll
from .hubspot.data_service import send_user_data
from .avatax.send_user_data import commit_transaction


def mindedge_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    status_data = {'comment': 'received', 'data': data}
    save_status_to_mongo(status_data)
    enroll(data)


def hubspot_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_user_data(data)


def avatax_callback(ch, method, properties, body):
    print('received task for avatax: ')
    data = json.loads(body.decode())
    print(data)
    commit_transaction(data)
