from django_scopes import scopes_disabled
from decouple import config
import requests
import base64
from django.utils import timezone

from django_initializer import initialize_django
initialize_django()

from shared_models.models import PaymentRefund


def tax_create(data):
    accountid = config('AVATAX_ACCOUNT_ID')
    license_key = config('AVATAX_LICENSE_KEY')
    tax_code = config('AVATAX_TAX_CODE')
    company_code = config('AVATAX_COMPANY_CODE')
    if data['product']['tax_code'] is not None or data['product']['tax_code'] == '':
        tax_code = data['product']['tax_code']

    auth_str = base64.b64encode(f'{accountid}:{license_key}'.encode('ascii')).decode('ascii')

    auth_header = {'Authorization': f'Basic {auth_str}'}

    url = config('AVATAX_URL')

    payload = {
        'addresses': {
            'shipTo': {
                'country': 'US',
                'postalCode': data['address']['zip_code']
            },
            'shipFrom': {
                'country': 'US',
                'postalCode': '02199'
            }
        },
        'type': 'SalesInvoice',
        'companyCode': company_code,
        'date': timezone.now().strftime("%Y-%m-%d"),
        'customerCode': data['primary_email'],
        'lines': [
            {
                'number': 1,
                'amount': data['price'],
                'taxCode': tax_code,
                'description': data['description']
            }
        ],
        'code': data['cart_id'],
        'commit': True
    }


    resp = requests.post(url, json=payload, headers=auth_header)

    return resp.status_code


def tax_refund(data):
    with scopes_disabled():
        try:
            refund = PaymentRefund.objects.get(id=data['refund_id'])
            del data['refund_id']
        except PaymentRefund.DoesNotExist:
            return {}

    accountid = config('AVATAX_ACCOUNT_ID')
    license_key = config('AVATAX_LICENSE_KEY')

    auth_str = base64.b64encode(f'{accountid}:{license_key}'.encode('ascii')).decode('ascii')

    auth_header = {'Authorization': f'Basic {auth_str}'}

    url = config('AVATAX_URL')

    company_code = config('AVATAX_COMPANY_CODE')
    cart_id = data['refundTransactionCode']

    splits = url.split('/')
    url = '/'.join(splits[:-2]) + f'/companies/{company_code}/transactions/{cart_id}/refund'

    resp = requests.post(url, json=data, headers=auth_header)

    refund.task_tax_refund = PaymentRefund.TASK_STATUS_FAILED
    if resp.status_code == 200:
        refund.task_tax_refund = PaymentRefund.TASK_STATUS_DONE
    return resp
