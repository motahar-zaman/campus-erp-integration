import requests
from campuslibs.loggers.mongo import save_to_mongo
from decouple import config
from django_scopes import scopes_disabled
from django_initializer import initialize_django
from django.core.exceptions import ValidationError


initialize_django()
from shared_models.models import Profile, PaymentRefund


def add_or_update_user(data):
    hubspot_portal_id = config('HUBSPOT_PORTAL_ID')
    hubspot_cart_creation_form_id = config('HUBSPOT_CONTACT_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{hubspot_portal_id}/' \
          f'{hubspot_cart_creation_form_id}'

    try:
        profile_id = data['profile_id']
    except KeyError:
        save_to_mongo(data={'type': 'crm-hubspot', 'comment': 'unknown data format'},
                      collection='enrollment_status_history')
        return

    try:
        profile = Profile.objects.get(id=profile_id)
    except Profile.DoesNotExist:
        save_to_mongo(data={'type': 'crm-hubspot', 'comment': 'profile not found'},
                      collection='enrollment_status_history')
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
        save_to_mongo(data={'type': 'crm-hubspot', 'comment': 'success', 'data': resp.json()},
                      collection='enrollment_status_history')
    else:
        save_to_mongo(data={'type': 'crm-hubspot', 'comment': 'failed', 'data': resp.json()},
                      collection='enrollment_status_history')

    return resp.status_code


def add_or_update_product(data):
    hubspot_portal_id = config('HUBSPOT_PORTAL_ID')
    hubspot_cart_creation_form_id = config('HUBSPOT_CART_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{hubspot_portal_id}/' \
          f'{hubspot_cart_creation_form_id}'

    refund = None
    try:
        refund_id = data['refund_id']
    except KeyError:
        pass
    else:
        with scopes_disabled():
            try:
                refund = PaymentRefund.objects.get(id=refund_id)
            except PaymentRefund.DoesNotExist:
                refund = None
            except ValidationError:
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
