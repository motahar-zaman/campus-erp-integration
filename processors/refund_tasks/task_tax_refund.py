from decouple import config
import os
import django
import requests
import base64
from django_scopes import scopes_disabled


# Django stuff begins
DEBUG = True
SECRET_KEY = '4l0ngs3cr3tstr1ngw3lln0ts0l0ngw41tn0w1tsl0ng3n0ugh'
ROOT_URLCONF = __name__
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

urlpatterns = []
PAYMENT_LIB_DIR = BASE_DIR

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'shared_models',
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': config('DATABASE_NAME', ''),
        'USER': config('DATABASE_USER', ''),
        'PASSWORD': config('DATABASE_PASSWORD', ''),
        'HOST': config('DATABASE_HOST', ''),
        'PORT': config('DATABASE_PORT', ''),
    }
}

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_USE_TLS = True
EMAIL_PORT = 587
EMAIL_HOST_USER = 'sakib@sgcsoft.net'
EMAIL_HOST_PASSWORD = 'howyouturnthison'
EMAIL_RECIPIENT_LIST = ['sakibccr@gmail.com']

os.environ.setdefault('DJANGO_SETTINGS_MODULE', __name__)
django.setup()

from shared_models.models import PaymentRefund
# Django stuff ends


def send_tax_refund_data(data):
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

    print(resp.json())

    return resp
