from campuslibs.loggers.mongo import save_to_mongo
from django.core.mail import send_mail
from services.mindedge import MindEdgeService
from decouple import config
from django_scopes import scopes_disabled
from .payments import payment_transaction
from django_initializer import initialize_django
initialize_django()

from shared_models.models import CourseEnrollment, CertificateEnrollment, LMSAccess, PaymentRefund, Certificate, Course, StoreConfiguration


def get_erp_config(erp, store):
    erp_config = {}

    try:
        store_configuration = store.store_configurations.get(
            external_entity__entity_type='enrollment_config',
            external_entity__entity_name__iexact=erp
        )
    except StoreConfiguration.DoesNotExist:
        save_to_mongo(data={'type': 'erp', 'comment': erp + ' not implemented'},
                      collection='enrollment_status_history')
        return 1

    erp_config['processor_class'] = MindEdgeService
    erp_config['credentials'] = store_configuration.config_value

    return erp_config


def enroll(message_data):
    if message_data['enrollment_type'] == 'course':
        enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CourseEnrollment.STATUS_PENDING

    else:
        enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CertificateEnrollment.STATUS_PENDING

    try:
        erp = message_data['erp']
        profile = message_data['profile']
        data = message_data['data']
        action = message_data['action']
        payment = message_data.pop('payment')
        store_payment_gateway = message_data.pop('store_payment_gateway')
    except KeyError:
        save_to_mongo(data={'type': 'erp', 'comment': 'unknown data format'},
                      collection='enrollment_status_history')
        return 1

    if erp == 'none':
        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CourseEnrollment.STATUS_SUCCESS
        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CertificateEnrollment.STATUS_SUCCESS

        enrollment.save()
        if store_payment_gateway:
            payment_transaction(payment, store_payment_gateway, 'priorAuthCaptureTransaction')
        return 1

    erp_config = get_erp_config(erp, store_payment_gateway.store)

    processor_class = erp_config['processor_class']
    credentials = erp_config['credentials']

    processor_obj = processor_class(credentials, profile, data)

    if not processor_obj.authenticate():
        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CourseEnrollment.STATUS_FAILED

        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CertificateEnrollment.STATUS_FAILED

        enrollment.save()
        status_data = {'type': 'erp', 'comment': 'authentication_failed', 'data': credentials}

        payment_transaction(payment, store_payment_gateway, 'voidTransaction')
        save_to_mongo(data=status_data, collection='enrollment_status_history')
        return 0

    status_data = {'type': 'erp', 'comment': 'erp_authenticated', 'data': credentials}
    save_to_mongo(data=status_data, collection='enrollment_status_history')
    action = getattr(processor_obj, action)
    resp = action()

    if resp['status'] == 'fail' and not resp['already_enrolled']:
        status_data = {'type': 'erp', 'comment': 'failed', 'data': resp}

        payment_transaction(payment, store_payment_gateway, 'voidTransaction')
        save_to_mongo(data=status_data, collection='enrollment_status_history')

        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CourseEnrollment.STATUS_FAILED

        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CertificateEnrollment.STATUS_FAILED
        return 0

    status_data = {'type': 'erp', 'comment': 'enrolled', 'data': resp}
    save_to_mongo(data=status_data, collection='enrollment_status_history')

    if message_data['enrollment_type'] == 'course':
        enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])

        already_enrolled = False
        try:
            already_enrolled = resp['already_enrolled']
        except KeyError:
            pass

        if already_enrolled:
            # lms says this course was already enrolled to.
            # so there must be another entry in the CourseEnrollment table with the same course for the same profile.
            try:
                old_enrollment = CourseEnrollment.objects.exclude(id=enrollment.id).get(profile=enrollment.profile, course=enrollment.course, section=enrollment.section)
            except CourseEnrollment.DoesNotExist:
                # not found. therefore proceed with the current enrollment obj
                pass
            else:
                # found. so lets delete current enrollment object. and use the found one instead.
                enrollment.delete()
                enrollment = old_enrollment

        enrollment.status = CourseEnrollment.STATUS_SUCCESS

        status_data = {'type': 'erp', 'comment': 'lms_created'}
        save_to_mongo(data=status_data, collection='enrollment_status_history')

        LMSAccess.objects.update_or_create(
            course_enrollment=enrollment,
            defaults={
                'student_ref': 'student',
                'lms_access_details': resp,
            }
        )
        #########################################
        # initiate payment capture              #
        #########################################
        payment_transaction(payment, store_payment_gateway, 'priorAuthCaptureTransaction')
    else:
        print('enrollment successful')
        enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CertificateEnrollment.STATUS_SUCCESS

    enrollment.save()


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
