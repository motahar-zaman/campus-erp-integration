from django_initializer import initialize_django
initialize_django()

from django_scopes import scopes_disabled
from shared_models.models import PaymentRefund, Cart, StoreCourseSection, Product
from django.utils import timezone


class TaxFormatter(object):
    def tax_create(self, payload):
        with scopes_disabled():
            try:
                cart = Cart.objects.get(id=payload['cart_id'])
            except Cart.DoesNotExist:
                return {}

            try:
                product = Product.objects.get(id=payload['product_id'])
            except Product.DoesNotExist:
                return {}

            description = ''
            try:
                store_course_section = StoreCourseSection.objects.get(id=payload['store_course_section_id'])
                description = store_course_section.store_course.course.title + ' (' + store_course_section.store_course.course.course_provider.name + ')'
            except StoreCourseSection.DoesNotExist:
                pass

        address = cart.profile.profileaddress_set.all().first()

        data = {
            'address': {'zip_code': address.zip_code},
            'price': str(cart.extended_amount - cart.discount_amount),
            'product': {'tax_code': product.tax_code, 'id': str(product.id)},
            'primary_email': cart.profile.primary_email,
            'cart_id': str(cart.id),
            'description': description
        }

        return data

    def tax_refund(self, payload):
        with scopes_disabled():
            try:
                refund = PaymentRefund.objects.get(id=payload['refund_id'])
            except PaymentRefund.DoesNotExist:
                return {}

        data = {
            'refund_id': str(refund.id),
            'refundTransactionCode': str(refund.payment.cart.id),
            'refundDate': timezone.now().strftime("%Y-%m-%d"),
            'refundType': 'Full',
            'referenceCode': 'Refund for a commited transaction'
        }

        return data
