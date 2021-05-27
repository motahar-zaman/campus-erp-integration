import requests
import base64
from django.utils import timezone
from decouple import config


def commit_transaction(data):
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
        'customerCode': data['profile']['id'],
        'lines': [
            {
                'number': 1,
                'amount': data['price'],
                'taxCode': tax_code,
                'description': data['product']['id']
            }
        ],
        'code': data['cart_id'],
        'commit': True
    }

    print('-------------payload--------------')
    print(payload)
    print('----------------------------------')

    resp = requests.post(url, json=payload, headers=auth_header)
    print(resp.json())

    return resp.status_code
