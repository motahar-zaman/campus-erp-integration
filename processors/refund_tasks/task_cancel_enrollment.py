from decouple import config
import os
import django
from django.core.mail import send_mail


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

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_USE_TLS = True
EMAIL_PORT = 587
EMAIL_HOST_USER = 'sakib@sgcsoft.net'
EMAIL_HOST_PASSWORD = 'howyouturnthison'
EMAIL_RECIPIENT_LIST = ['sakibccr@gmail.com']

os.environ.setdefault('DJANGO_SETTINGS_MODULE', __name__)
django.setup()

from shared_models.models import PaymentRefund
# Django stuff ends


def send_enrollment_cancel_email(data):
    refund = PaymentRefund.objects.get(id=data['refund_id'])
    student_name = data['student_name']
    student_email = data['student_email']
    certificate = data['certificate']
    course = data['course']

    subject = 'Enrollment cancellation request'
    message = f'Student name: {student_name}, Email: {student_email}, certificate: {certificate}, course: {course}'

    email_from = EMAIL_HOST_USER
    recipient_list = EMAIL_RECIPIENT_LIST
    try:
        send_mail(subject, message, email_from, recipient_list, fail_silently=False)
    except:
        refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_DONE
    else:
        refund.task_cancel_enrollment = PaymentRefund.TASK_STATUS_FAILED

    refund.save()
    return refund
