from django_initializer import initialize_django
initialize_django()

from shared_models.models import PaymentRefund, CartItem, StoreCertificate, StoreCourseSection, Profile
from django_scopes import scopes_disabled


class EnrollmentFormatter(object):
    def enroll(self, payload):
        try:
            profile = Profile.objects.get(id=payload['profile_id'])
        except Profile.DoesNotExist:
            return {}

        data = {
            'data': {
                'cid': payload['external_id']
            },
            'erp': 'mindedge',
            'profile': {'primary_email': profile.primary_email, 'first_name': profile.first_name, 'last_name': profile.last_name},
            'action': 'enroll',
            'enrollment_type': 'course',
            'enrollment_id': payload['course_enrollment_id'],
            'cart_id': payload['cart_id']
        }
        return data

    def unenroll(self, payload):
        with scopes_disabled():
            try:
                refund = PaymentRefund.objects.get(id=payload['refund_id'])
            except KeyError:
                return {}
            except PaymentRefund.DoesNotExist:
                return {}

            try:
                cart_item = refund.payment.cart.cart_items.first()
            except CartItem.DoesNotExist:
                certificate_id = ''
                course_id = ''
            else:
                try:
                    store_certificate = StoreCertificate.objects.get(product=cart_item.product)
                except (StoreCertificate.DoesNotExist, StoreCertificate.MultipleObjectsReturned):
                    certificate_id = ''
                else:
                    certificate_id = str(store_certificate.certificate.id)

                try:
                    store_course_section = StoreCourseSection.objects.get(product=cart_item.product)
                except (StoreCourseSection.DoesNotExist, StoreCourseSection.MultipleObjectsReturned):
                    course_id = ''
                else:
                    course_id = str(store_course_section.store_course.course.id)

        data = {
            'refund_id': str(refund.id),
            'student_name': f'{refund.payment.cart.profile.first_name} {refund.payment.cart.profile.last_name}',
            'student_email': refund.payment.cart.profile.primary_email,
            'certificate': certificate_id,
            'course': course_id
        }

        return data
