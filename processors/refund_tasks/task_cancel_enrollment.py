from decouple import config
import os
import django
from django.core.mail import send_mail
from django_scopes import scopes_disabled


# Django stuff begins
DEBUG = True
SECRET_KEY = '4l0ngs3cr3tstr1ngw3lln0ts0l0ngw41tn0w1tsl0ng3n0ugh'
ROOT_URLCONF = __name__
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

urlpatterns = []
PAYMENT_LIB_DIR = BASE_DIR

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'shared_models',
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': config('DATABASE_NAME', ''),
        'USER': config('DATABASE_USER', ''),
        'PASSWORD': config('DATABASE_PASSWORD', ''),
        'HOST': config('DATABASE_HOST', ''),
        'PORT': config('DATABASE_PORT', ''),
    }
}

os.environ.setdefault('DJANGO_SETTINGS_MODULE', __name__)
django.setup()

from shared_models.models import PaymentRefund, Certificate, Course
# Django stuff ends


def save_task_data(msg, data, err={}):
    from mongoengine import connect, disconnect, get_db
    from decouple import config, UndefinedValueError

    try:
        mongodb_host = config('MONGODB_HOST')
        mongodb_database = config('MONGODB_DATABASE')
        mongodb_port = config('MONGODB_PORT')
        mongodb_username = config('MONGODB_USERNAME')
        mongodb_password = config('MONGODB_PASSWORD')
        mongodb_auth_database = config('MONGODB_AUTH_DATABASE')
    except UndefinedValueError:
        print('----> ', msg, data)
        return

    disconnect()
    connect(mongodb_database, host=mongodb_host, port=int(mongodb_port), username=mongodb_username, password=mongodb_password, authentication_source=mongodb_auth_database)

    db = get_db()
    coll = db.get_collection('debug')
    coll.insert_one({'message': msg, 'data': data, 'error': err})


def send_enrollment_cancel_email(data):
    save_task_data('enrollment cancel task received', data)
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
            save_task_data('enrollment cancel task email sending failed', data, str(e))
            refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_FAILED
        else:
            save_task_data('enrollment cancel task email sending succeeded', data)
            refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_DONE
    else:
        save_task_data('course provider has no refund email', data)
        refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_FAILED
    refund.save()
    return refund
