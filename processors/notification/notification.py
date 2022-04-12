from mongoengine import get_db, connect, disconnect
from bson import ObjectId
from decouple import config
from django.utils import timezone
from shared_models.models import Notification, NotificationLog, Event, Payment, Cart, EventSubscription, Partner
from django_scopes import scopes_disabled
import requests


def notification_to_course_provider(notification_id):
    print('notification_to_course_provider')
    try:
        notification = Notification.objects.get(pk=notification_id)
    except Notification.DoesNotExist:
        print('notification DoesNotExist')
        return False
    else:
        notification_type = notification.data['type']
        ref_id = notification.data['id']

        if notification_type == 'payment':
            print('payment type notification')
            with scopes_disabled():
                try:
                    payment = Payment.objects.get(pk=ref_id)
                except Payment.DoesNotExist:
                    print('payment DoesNotExist')
                    return False
                else:
                    order = payment.cart
        else:
            print('order type notification')
            with scopes_disabled():
                try:
                    order = Cart.objects.get(pk=ref_id)
                except Cart.DoesNotExist:
                    print('order DoesNotExist')
                    return False

        print("order id = " + str(order.id))
        print(order.cart_items.all())

        for item in order.cart_items.all():
            print('in cart_items')
            course_provider = item.product.store_course_section.store_course.course.course_provider
            try:
                partner = Partner.objects.get(ref_id=course_provider.id)
            except Partner.DoesNotExist:
                print('partner DoesNotExist')
                return False

            #check if partner is subscribed or not for the event
            #if subscribed, send notification message
            subscribed = None
            try:
                subscribed = EventSubscription.objects.get(partner=partner, event=notification.event)
            except EventSubscription.DoesNotExist:
                print('subscribed DoesNotExist')
                pass

            if subscribed:
                print('subscribed')
                url = partner.notification_submission_url

                data = {
                    'notification': str(notification.id),
                    'notification_type': notification_type,
                    'message': 'you got an '+notification_type
                }
                send_msg, response = send_message_to_course_provider(url, data)
                if send_msg:
                    notification.status = Notification.STATUS_SUCCESSFUL
                else:
                    notification.status = Notification.STATUS_FAILED
                notification.save()
                print('notification status saved, log creating')

                notification_log = NotificationLog.objects.create(
                    notification=notification,
                    partner=partner,
                    log_time=timezone.now(),
                    status=notification.status,
                    http_response=response
                )
                print('notification log created')
        print('notification processing complete')
    return True


def send_message_to_course_provider(url, data):
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=data)
    return True, response.json()