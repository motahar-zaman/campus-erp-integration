import requests
from decouple import config
from shared_models.models import Profile
from status_logger import save_status_to_mongo


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
    HUBSPOT_CART_UPDATE_FORM_ID = config('HUBSPOT_CART_UPDATE_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CART_CREATION_FORM_ID}'

    for field in data['fields']:
        if field['name'] == 'cart_status' and field['value'].lower() == 'processed':
            url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CART_UPDATE_FORM_ID}'
            break

    resp = requests.post(url, json=data)

    return resp.status_code
