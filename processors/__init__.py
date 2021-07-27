import json

from formatters.enrollment import EnrollmentFormatter
from formatters.crm import CRMFormatter
from formatters.tax import TaxFormatter

from processors.enrollment.mindedge import enroll, unenroll
from processors.crm.hubspot import add_or_update_user, add_or_update_product
from processors.tax.avatax import tax_create, tax_refund

from loggers.elastic_search import upload_log


def requestlog_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    upload_log(data)


def enroll_callback(ch, method, properties, body):
    payload = json.loads(body.decode())

    if 'enrollment' in method.routing_key:
        formatter = EnrollmentFormatter()
        data = formatter.enroll(payload)
        enroll(data)

    if 'crm_user' in method.routing_key:
        formatter = CRMFormatter()
        data = formatter.add_or_update_user(payload)
        add_or_update_user(data)

    if 'crm_product' in method.routing_key:
        formatter = CRMFormatter()
        data = formatter.add_or_update_product(payload)
        add_or_update_product(data)

    if 'tax' in method.routing_key:
        tax_create(data)


def refund_callback(ch, method, properties, body):
    payload = json.loads(body.decode())

    if 'email' in method.routing_key:
        formatter = EnrollmentFormatter()
        data = formatter.unenroll(payload)
        unenroll(data)

    if 'crm_product' in method.routing_key:
        formatter = CRMFormatter()
        data = formatter.add_or_update_product(payload)
        add_or_update_product(data)

    if 'tax' in method.routing_key:
        formatter = TaxFormatter()
        data = formatter.tax_refund(payload)
        tax_refund(data)
