import requests
from decouple import config
from shared_models.models import Profile
from status_logger import save_status_to_mongo
import os
import django
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

os.environ.setdefault('DJANGO_SETTINGS_MODULE', __name__)
django.setup()

from shared_models.models import Cart, PaymentRefund
# Django stuff ends


def update_refund_status(data):
    cart_id = ''
    for item in data['fields']:
        if item['name'] == 'cart_id':
            cart_id = item['value']
            break

    try:
        with scopes_disabled():
            cart = Cart.objects.get(id=cart_id)
    except Cart.DoesNotExist:
        pass
    else:
        with scopes_disabled():
            refund = PaymentRefund.objects.filter(payment__cart=cart).first()
        refund.task_crm_update = PaymentRefund.TASK_STATUS_DONE
        refund.save()


def send_user_data(data):
    HUBSPOT_PORTAL_ID = config('HUBSPOT_PORTAL_ID')
    HUBSPOT_CONTACT_CREATION_FORM_ID = config('HUBSPOT_CONTACT_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CONTACT_CREATION_FORM_ID}'

    try:
        profile_id = data['profile_id']
    except KeyError:
        save_status_to_mongo({'comment': 'unknown data format'})
        return

    try:
        profile = Profile.objects.get(id=profile_id)
    except Profile.DoesNotExist:
        save_status_to_mongo({'comment': 'profile not found'})
        return

    data = {
        'fields': [{
            'name': 'email',
            'value': profile.primary_email
        }, {
            'name': 'firstname',
            'value': profile.first_name
        }, {
            'name': 'lastname',
            'value': profile.last_name
        }, {
            'name': 'country',
            'value': 'USA'
        }, {
            'name': 'terms_accepted',
            'value': True
        }, {
            'name': 'opt_in_to_newsletter',
            'value': True
        }],
        'context': {
            'hutk': data['hubspot_token']
        }
    }

    resp = requests.post(url, json=data)
    if resp.status_code == 200:
        save_status_to_mongo({'comment': 'success', 'data': resp.json()})

    else:
        save_status_to_mongo({'comment': 'failed', 'data': resp.json()})

    return resp.status_code


def send_product_data(data):
    HUBSPOT_PORTAL_ID = config('HUBSPOT_PORTAL_ID')
    HUBSPOT_CART_CREATION_FORM_ID = config('HUBSPOT_CART_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CART_CREATION_FORM_ID}'

    resp = requests.post(url, json=data)

    if resp.status_code == 200:
        for item in data['fields']:
            if item['name'] == 'cart_status':
                if item['value'] == 'refunded':
                    update_refund_status(data)
                    break

    return resp.status_code
