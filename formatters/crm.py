from config.mongo_client import connect_mongodb, disconnect_mongodb
from django_initializer import initialize_django
initialize_django()

from shared_models.models import Cart, StoreCourseSection, StoreCertificate, Payment, PaymentRefund
from models.course.course import Course as CourseModel

from django_scopes import scopes_disabled


def get_details(product):
    section_details = {'name': None, 'start_date': None, 'end_date': None, 'execution_mode': None}

    product_type = ''
    product_name = ''
    course_provider = ''

    try:
        connect_mongodb()
        try:
            with scopes_disabled():
                store_course_section = StoreCourseSection.objects.get(product=product)
            product_type = 'Course'
            product_name = store_course_section.store_course.course.title
            course_provider = store_course_section.store_course.course.course_provider.name

        except StoreCourseSection.DoesNotExist:
            pass
        else:
            start_date = None
            end_date = None
            execution_mode = None

            try:
                course_model = CourseModel.objects.get(id=store_course_section.store_course.course.content_db_reference)
                for section in course_model.sections:
                    if section.code == store_course_section.section.name:
                        start_date = section.start_date
                        end_date = section.end_date
                        execution_mode = section.execution_mode
                        break

                section_details = {'name': store_course_section.section.name, 'start_date': str(start_date), 'end_date': str(end_date), 'execution_mode': execution_mode}
            except CourseModel.DoesNotExist:
                pass
        try:
            with scopes_disabled():
                store_certificate = StoreCertificate.objects.get(product=product)
            product_type = 'Certificate'
            product_name = store_certificate.certificate.title
            course_provider = store_certificate.certificate.course_provider.name
        except StoreCertificate.DoesNotExist:
            pass
    finally:
        disconnect_mongodb()

    return {'section': section_details, 'product_type': product_type, 'product_name': product_name, 'course_provider': course_provider}


class CRMFormatter(object):
    def add_or_update_user(self, payload):
        return {'profile_id': payload['profile_id'], 'hubspot_token': payload['hubspot_token']}

    def add_or_update_product(self, payload):
        if payload['refund_id']:
            with scopes_disabled():
                try:
                    refund = PaymentRefund.objects.get(id=payload['refund_id'])
                except PaymentRefund.DoesNotExist:
                    return {}

            payment = refund.payment
            cart = payment.cart

            cart_status = 'refunded'
            refund_id = payload['refund_id']

        else:
            with scopes_disabled():
                try:
                    cart = Cart.objects.get(id=payload['cart_id'])
                except Cart.DoesNotExist:
                    return {}

            cart_status = cart.status
            refund_id = ''
            with scopes_disabled():
                try:
                    payment = Payment.objects.get(id=payload['payment_id'])
                except Payment.DoesNotExist:
                    payment = None

        if cart.cart_items.all().exists():
            item = cart.cart_items.first()  # since all cart will have only one item
            product = item.product
        else:
            return 0  # if cart doesn't have a item, what product can you send?

        zip_code = ''
        if cart.profile.profileaddress_set.all().exists():
            address = cart.profile.profileaddress_set.first()
            zip_code = address.zip_code

        details = get_details(product)

        data = {
            'refund_id': refund_id,
            'fields': [
                {
                    'name': 'email',
                    'value': cart.profile.primary_email
                },
                {
                    'name': 'product_type',
                    'value': details['product_type']
                },
                {
                    'name': 'product_id',
                    'value': str(product.id)
                },
                {
                    'name': 'product_name',
                    'value': details['product_name']
                },
                {
                    'name': 'cart_status',
                    'value': cart_status
                },
                {
                    'name': 'cart_url',
                    'value': f'https://enrollment.campus.com/checkout/{cart.id}/'
                },
                {
                    'name': 'cart_id',
                    'value': str(cart.id)
                },
                {
                    'name': 'course_provider_name',
                    'value': details['course_provider']
                },
                {
                    'name': 'product_fee',
                    'value': str(product.fee)
                },
                {
                    'name': 'section_name',
                    'value': details['section']['name']
                },

                {
                    'name': 'section_start_date',
                    'value': details['section']['start_date']
                },

                {
                    'name': 'section_end_date',
                    'value': details['section']['end_date']
                },
                {
                    'name': 'execution_mode',
                    'value': details['section']['execution_mode']
                },
                {
                    'name': 'extended_amount',
                    'value': float(cart.extended_amount)
                },
                {
                    'name': 'discount_amount',
                    'value': float(cart.discount_amount)
                },
                {
                    'name': 'sales_tax',
                    'value': float(cart.sales_tax)
                },
                {
                    'name': 'total_amount',
                    'value': float(cart.total_amount)
                },
                {
                    'name': 'coupon_code',
                    'value': cart.coupon.code if cart.coupon else ''
                },
                {
                    'name': 'transaction_reference',
                    'value': str(payment.auth_code) if payment else ''
                },
                {
                    'name': 'card_type',
                    'value': str(payment.card_type) if payment else ''
                },
                {
                    'name': 'card_number',
                    'value': str(payment.card_number) if payment else ''
                },
                {
                    'name': 'billing_zip_code',
                    'value': zip_code
                }
            ]
        }

        return data
