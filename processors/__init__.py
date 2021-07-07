import json
from status_logger import save_status_to_mongo
from .mindedge.enrollment import enroll
from .hubspot.data_service import send_user_data, send_product_data
from .avatax.send_user_data import commit_transaction

from .elastic_search.data_logger import upload_log

from .refund_tasks.task_cancel_enrollment import send_enrollment_cancel_email
from .refund_tasks.task_crm_update import send_cart_data
from .refund_tasks.task_tax_refund import send_tax_refund_data


def mindedge_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    status_data = {'comment': 'received', 'data': data}
    save_status_to_mongo(status_data)
    enroll(data)


def hubspot_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_user_data(data)


def product_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_product_data(data)


def avatax_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    commit_transaction(data)


def requestlog_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    upload_log(data)


def send_enrollment_cancel_email_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_enrollment_cancel_email(data)


def send_cart_data_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_cart_data(data)


def send_tax_refund_data_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    send_tax_refund_data(data)
