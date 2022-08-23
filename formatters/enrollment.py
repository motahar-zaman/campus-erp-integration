from django_initializer import initialize_django
initialize_django()

from shared_models.models import Payment, QuestionBank, StudentProfile, Cart, CourseEnrollment, PaymentRefund, CartItem, StoreCertificate, StoreCourseSection, Profile, StorePaymentGateway
from django_scopes import scopes_disabled
from django.core.exceptions import ValidationError


class EnrollmentFormatter(object):
    def mindedge(self, profile, external_id, course_enrollment, payment, payload):
        with scopes_disabled():
            try:
                store_payment_gateway = StorePaymentGateway.objects.get(id=payload['store_payment_gateway_id'])
            except StorePaymentGateway.DoesNotExist:
                store_payment_gateway = None

        return {
            'data': {
                'cid': external_id,
                'login_link': True
            },
            'store': {
                'slug': course_enrollment.store.url_slug,
                'name': course_enrollment.store.name
            },
            'profile': {'primary_email': profile.primary_email, 'first_name': profile.first_name, 'last_name': profile.last_name},
            'action': 'enroll',
            'enrollment_type': 'course',
            'enrollment_id': str(course_enrollment.id),
            'course_enrollment': course_enrollment,
            'cart_id': payload['cart_id'],
            'payment': payment,
            'store_payment_gateway': store_payment_gateway
        }

    def j1(self, profile, external_id, course_enrollment, payment):
        registration_details = {}
        reg_info = {}
        # getting registration info
        for reg_detail in payment.cart.registration_details:
            try:
                if profile.primary_email == reg_detail['student'] and str(course_enrollment.cart_item.product.id) == reg_detail['product_id']:
                    reg_info = reg_detail['data']
            except KeyError:
                reg_info = {}

        for key, val in reg_info.items():
            try:
                question = QuestionBank.objects.get(id=key)
            except (QuestionBank.DoesNotExist, ValidationError):
                continue
            registration_details[question.external_id] = val

        # getting student id
        school_student_id = ''
        student_profiles = StudentProfile.objects.filter(profile=profile)
        if student_profiles.exists():
            school_student_id = student_profiles.first().external_profile_id

        # getting profile info
        extra_info = {}
        profile_details = {}

        for profile_data in payment.cart.student_details:
            try:
                if profile.primary_email == profile_data['email'] and str(course_enrollment.cart_item.product.id) == profile_data['product_id']:
                    extra_info = profile_data['extra_info']
            except KeyError:
                extra_info = {}

        for key, val in extra_info.items():
            try:
                question = QuestionBank.objects.get(id=key)
            except (QuestionBank.DoesNotExist, ValidationError):
                continue
            profile_details[question.external_id] = val

        return {
            'external_id': external_id,
            'enrollment_id': str(course_enrollment.ref_id),
            'product_type': 'section',
            'registration_details': registration_details,
            'student': {
                'school_student_id': school_student_id,
                'email': profile.primary_email,
                'first_name': profile.first_name,
                'last_name': profile.last_name,
                'profile_details': profile_details,
            },
        }

    def enroll(self, payload):
        mindedge_data = []
        hir_data = {
            'enrollments': []
        }
        common_data = []
        j1_data = {
            'enrollments': []
        }

        mindedge_config = {}
        j1_config = {}
        hir_config = {}
        common_config = {}

        payment = None
        try:
            with scopes_disabled():
                payment = Payment.objects.get(id=payload['payment_id'])

            for profile_id, external_id, enrollment_id in zip(payload['profile_id'], payload['external_id'], payload['course_enrollment_id']):
                with scopes_disabled():
                    try:
                        profile = Profile.objects.get(id=profile_id)
                    except Profile.DoesNotExist:
                        continue

                    try:
                        course_enrollment = CourseEnrollment.objects.get(id=enrollment_id)
                    except CourseEnrollment.DoesNotExist:
                        continue
                    else:
                        course_enrollment.status = CourseEnrollment.STATUS_PENDING
                        course_enrollment.save()

                try:
                    enrollment_url = course_enrollment.course.course_provider.configuration['enrollment_url']
                except KeyError:
                    enrollment_url = 'http://PDSVC-UNITY.JENZABARCLOUD.COM:9090/ws/rest/campus/api/enrollment/create' #j1 provider url

                if course_enrollment.course.course_provider.configuration.get('erp', '') == 'mindedge':
                    mindedge_data.append(self.mindedge(profile, external_id, course_enrollment, payment, payload))
                    mindedge_config = course_enrollment.course.course_provider.configuration

                elif course_enrollment.course.course_provider.configuration.get('erp', '') == 'j1':
                    j1_config = course_enrollment.course.course_provider.configuration
                    j1_data['order_id'] = str(payment.cart.order_ref)
                    j1_data['enrollments'].append(self.j1(profile, external_id, course_enrollment, payment))
                    j1_data['store']= {
                        'slug': course_enrollment.store.url_slug,
                        'name': course_enrollment.store.name
                    }
                    j1_data['payment'] = {
                        'amount': str(payment.amount),
                        'currency_code': payment.currency_code,
                        'transaction_reference': payment.transaction_reference,
                        'auth_code': payment.auth_code,
                        'payment_type': payment.payment_type,
                        'bank': payment.bank,
                        'account_number': payment.account_number,
                        'card_type': payment.card_type,
                        'card_number': payment.card_number,
                        'reason_code': payment.reason_code,
                        'reason_description': payment.reason_description,
                        'customer_ip': payment.customer_ip,
                        'status': payment.status,
                        'transaction_time': str(payment.transaction_time),
                    }
                    agreement_details = {}
                    for key, val in payment.cart.agreement_details.items():
                        try:
                            question = QuestionBank.objects.get(id=key)
                        except (QuestionBank.DoesNotExist, ValidationError):
                            continue
                        agreement_details[question.external_id] = val
                    j1_data['agreement_details'] = agreement_details

                elif course_enrollment.course.course_provider.configuration.get('erp', '') == 'hir':
                    hir_config = course_enrollment.course.course_provider.configuration
                    hir_data['order_id'] = str(payment.cart.order_ref)
                    hir_data['enrollments'].append(self.j1(profile, external_id, course_enrollment, payment))
                    hir_data['store'] = {
                        'slug': course_enrollment.store.url_slug,
                        'name': course_enrollment.store.name
                    }
                    hir_data['payment'] = {
                        'amount': str(payment.amount),
                        'currency_code': payment.currency_code,
                        'transaction_reference': payment.transaction_reference,
                        'auth_code': payment.auth_code,
                        'payment_type': payment.payment_type,
                        'bank': payment.bank,
                        'account_number': payment.account_number,
                        'card_type': payment.card_type,
                        'card_number': payment.card_number,
                        'reason_code': payment.reason_code,
                        'reason_description': payment.reason_description,
                        'customer_ip': payment.customer_ip,
                        'status': payment.status,
                        'transaction_time': str(payment.transaction_time),
                    }
                    agreement_details = {}
                    for key, val in payment.cart.agreement_details.items():
                        try:
                            question = QuestionBank.objects.get(id=key)
                        except (QuestionBank.DoesNotExist, ValidationError):
                            continue
                        agreement_details[question.external_id] = val
                    hir_data['agreement_details'] = agreement_details

                else:
                    common_data.append(self.mindedge(profile, external_id, course_enrollment, payment, payload))
                    common_config = course_enrollment.course.course_provider.configuration

        except Payment.DoesNotExist:
            pass

        return {
            'erp_list':[{
                'erp': 'mindedge',
                'data': mindedge_data,
                'config': mindedge_config
            }, {
                'erp': 'j1',
                'data': j1_data,
                'config': j1_config
            }, {
                'erp': 'hir',
                'data': hir_data,
                'config': hir_config
            }, {
                'erp': 'none',
                'data': common_data,
                'config': common_config
            }],
            'payment': payment,
            'store_payment_gateway_id': payload['store_payment_gateway_id']
        }


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
