from django.core.mail import send_mail
from services.mindedge import MindEdgeService
from decouple import config
from loggers.mongo import save_status_to_mongo
from django_scopes import scopes_disabled
from django_initializer import initialize_django
initialize_django()

from shared_models.models import CourseEnrollment, CertificateEnrollment, LMSAccess, PaymentRefund, Certificate, Course


configs = {
    'mindedge': {
        'processor_class': MindEdgeService,
        'credentials': {
            'username': config('MINDEDGE_USERNAME', 'jenzabar'),
            'password': config('MINDEDGE_PASSWORD', 'jz_me_api'),
            'token': config('MINDEDGE_TOKEN', '09d66f6e5482d9b0ba91815c350fd9af3770819b'),
            'url': config('MINDEDGE_URL', 'https://api.mindedgeuniversity.com/v1/studentService'),
        }
    }
}


def enroll(message_data):
    print('attempting enrollment')
    if message_data['enrollment_type'] == 'course':
        print('enrollment type is course')
        enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CourseEnrollment.STATUS_PENDING

    else:
        enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CertificateEnrollment.STATUS_PENDING

    try:
        print('getting erp data')
        erp = message_data['erp']
        profile = message_data['profile']
        data = message_data['data']
        action = message_data['action']
    except KeyError:
        print('could not get erp data')
        save_status_to_mongo(status_data={'comment': 'unknown data format'})
        return 1

    try:
        print('getting erp config')
        erp_config = configs[erp]
    except KeyError:
        print('could not get erp config')
        save_status_to_mongo(status_data={'comment': erp + ' not implemented'})
        return 1

    processor_class = erp_config['processor_class']
    credentials = erp_config['credentials']

    processor_obj = processor_class(credentials, profile, data)

    print('authenticating erp')
    if not processor_obj.authenticate():
        print('authentication erp failed')
        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CourseEnrollment.STATUS_FAILED

        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CertificateEnrollment.STATUS_FAILED

        enrollment.save()
        status_data = {'comment': 'authentication_failed', 'data': credentials}
        save_status_to_mongo(status_data=status_data)
        return 0

    print('erp authentication successful')
    status_data = {'comment': 'erp_authenticated', 'data': credentials}
    save_status_to_mongo(status_data=status_data)
    action = getattr(processor_obj, action)
    print('Enrolling...')
    resp = action()
    print('enrollment response: ', resp)

    if resp['status'] == 'fail' and not resp['already_enrolled']:
        status_data = {'comment': 'failed', 'data': resp}
        save_status_to_mongo(status_data=status_data)

        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CourseEnrollment.STATUS_FAILED

        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
            enrollment.status = CertificateEnrollment.STATUS_FAILED
        return 0

    status_data = {'comment': 'enrolled', 'data': resp}
    save_status_to_mongo(status_data=status_data)

    if message_data['enrollment_type'] == 'course':
        enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])

        already_enrolled = False
        try:
            already_enrolled = resp['already_enrolled']
        except KeyError:
            pass

        if already_enrolled:
            print('this course was already enrolled')
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

        status_data = {'comment': 'lms_created'}
        save_status_to_mongo(status_data=status_data)

        LMSAccess.objects.update_or_create(
            course_enrollment=enrollment,
            defaults={
                'student_ref': 'student',
                'lms_access_details': resp,
            }
        )
    else:
        print('enrollment successful')
        enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])
        enrollment.status = CertificateEnrollment.STATUS_SUCCESS

    print('saving enrollment')
    enrollment.save()
    print('done')


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
