from campuslibs.loggers.mongo import save_to_mongo
from django.core.mail import send_mail
from decouple import config
from django_scopes import scopes_disabled
from .payments import payment_transaction
from django_initializer import initialize_django
initialize_django()

from shared_models.models import Certificate, Course, CourseEnrollment, PaymentRefund, StorePaymentGateway
from .mindedge import handle_mindedge_enrollment
from .j1 import handle_j1_enrollment

def enroll(enrollment_data):
    print('-----------------------------------------')
    print(enrollment_data)
    print('-----------------------------------------')
    payment = enrollment_data['payment']

    for item in enrollment_data['erp_list']:
        if item['erp'] == 'mindedge':
            for message_data in item['data']:
                try:
                    profile = message_data['profile']
                    erp_data = message_data['data']
                    handle_mindedge_enrollment(profile, erp_data, message_data, enrollment)
                except KeyError:
                    save_to_mongo(data={'type': 'erp', 'comment': 'unknown data format'},
                                collection='enrollment_status_history')
                    continue
        elif item['erp'] == 'j1':
            cart = payment.cart
            cart.enrollment_request = {'request': item['data']}
            cart.save()
            resp = handle_j1_enrollment(item['data'])
            cart.enrollment_request['response'] = resp
            cart.save()
        else:
            for message_data in item['data']:
                enrollment = message_data.pop('course_enrollment', None)
                if enrollment:
                    enrollment.status = CourseEnrollment.STATUS_SUCCESS
                    enrollment.save()
                else:
                    continue

    if payment.amount > 0.0:
        try:
            store_payment_gateway = StorePaymentGateway.objects.get(id=enrollment_data['store_payment_gateway_id'])
            payment_transaction(payment, store_payment_gateway, 'priorAuthCaptureTransaction')
        except StorePaymentGateway.DoesNotExist:
            pass


def unenroll(data):
    with scopes_disabled():
        refund = PaymentRefund.objects.get(id=data['refund_id'])
    student_name = data['student_name']
    student_email = data['student_email']
    certificate_id = data['certificate']
    course_id = data['course']

    subject = 'Enrollment cancellation request'
    message = f'Student name: {student_name}\nEmail: {student_email}'

    recipients = []

    if certificate_id != '':
        message = f'{message}\nCertificate: {certificate_id}'
        with scopes_disabled():
            certificate = Certificate.objects.get(id=certificate_id)

        if certificate.course_provider.refund_email is not None:
            recipients.append(certificate.course_provider.refund_email)

    if course_id != '':
        message = f'{message}\nCourse: {course_id}'
        with scopes_disabled():
            course = Course.objects.get(id=course_id)

        if course.course_provider.refund_email is not None:
            recipients.append(course.course_provider.refund_email)

    if len(recipients):
        try:
            from django.conf import settings
            settings.EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
            settings.EMAIL_HOST = config('SMTP_HOST')
            settings.EMAIL_USE_TLS = config('SMTP_USE_TLS', cast=bool)
            settings.EMAIL_PORT = config('SMTP_PORT', cast=int)
            settings.EMAIL_HOST_USER = config('SMTP_HOST_USER')
            settings.EMAIL_HOST_PASSWORD = config('SMTP_HOST_PASSWORD')
            send_mail(subject, message, config('SMTP_HOST_USER'), recipients, fail_silently=False)
        except Exception as e:
            refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_FAILED
        else:
            refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_DONE
    else:
        refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_FAILED
    refund.save()
    return refund
