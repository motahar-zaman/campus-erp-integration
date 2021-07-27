import requests
from decouple import config
from loggers.mongo import save_status_to_mongo
from django_scopes import scopes_disabled

from django_initializer import initialize_django
initialize_django()

from shared_models.models import Profile, PaymentRefund


def add_or_update_user(data):
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


def add_or_update_product(data):
    HUBSPOT_PORTAL_ID = config('HUBSPOT_PORTAL_ID')
    HUBSPOT_CART_CREATION_FORM_ID = config('HUBSPOT_CART_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CART_CREATION_FORM_ID}'

    refund_id = None
    refund = None

    try:
        refund_id = data['refund_id']
    except KeyError:
        pass
    else:
        try:
            with scopes_disabled():
                refund = PaymentRefund.objects.get(id=refund_id)
        except PaymentRefund.DoesNotExist:
            refund = None

        del data['refund_id']

    resp = requests.post(url, json=data)

    if resp.status_code == 200:
        for item in data['fields']:
            if item['name'] == 'cart_status':
                if item['value'] == 'refunded' and refund:
                    refund.task_crm_update = PaymentRefund.TASK_STATUS_DONE
                    refund.save()
                    break

    return resp.status_code
