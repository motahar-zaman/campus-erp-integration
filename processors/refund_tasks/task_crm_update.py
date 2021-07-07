import requests
from decouple import config


def send_cart_data(data):
    HUBSPOT_PORTAL_ID = config('HUBSPOT_PORTAL_ID')
    HUBSPOT_CART_CREATION_FORM_ID = config('HUBSPOT_CART_CREATION_FORM_ID')

    url = f'https://api.hsforms.com/submissions/v3/integration/submit/{HUBSPOT_PORTAL_ID}/{HUBSPOT_CART_CREATION_FORM_ID}'

    resp = requests.post(url, json=data)

    return resp
