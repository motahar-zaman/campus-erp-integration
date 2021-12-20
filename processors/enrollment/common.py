from campuslibs.loggers.mongo import save_to_mongo
from django.core.mail import send_mail
from decouple import config
from django_scopes import scopes_disabled
from .payments import payment_transaction
from django_initializer import initialize_django
initialize_django()

from shared_models.models import CourseEnrollment, CertificateEnrollment, LMSAccess, PaymentRefund, Certificate, Course, StoreConfiguration
from .mindedge import handle_mindedge_enrollment
from .j1 import handle_j1_enrollment

def enroll(enrollment_data):
    for data in enrollment_data:
        if data['erp'] == 'none' or data['erp'] == 'mindedge':
            for message_data in data['data']:
                enrollment = message_data.pop('course_enrollment', None)
                if enrollment:
                    enrollment.status = 'pending'
                    enrollment.save()
                else:
                    return 1

                try:
                    erp = message_data['erp']
                    profile = message_data['profile']
                    erp_data = message_data['data']
                    action = message_data['action']
                    payment = message_data.pop('payment')
                    store_payment_gateway = message_data.pop('store_payment_gateway')
                except KeyError:
                    save_to_mongo(data={'type': 'erp', 'comment': 'unknown data format'},
                                collection='enrollment_status_history')
                    return 1

                if message_data['erp'] == 'none':
                    enrollment.status = 'success'
                    enrollment.save()
                    print('erp none')

                if message_data['erp'] == 'mindedge':
                    handle_mindedge_enrollment(erp, profile, erp_data, message_data, enrollment)

        if data['erp'] == 'j1':
            payment = data['data'].pop('payment_obj', None)
            store_payment_gateway = data['data'].pop('store_payment_gateway_obj', None)
            handle_j1_enrollment(data['data'])

    if payment.amount > 0.0:
        payment_transaction(payment, store_payment_gateway, 'priorAuthCaptureTransaction')


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
