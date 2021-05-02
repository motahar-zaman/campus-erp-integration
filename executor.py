import os
import django
from processors.mindedge import MindEdgeService
from decouple import config
from status_history import save_status_history

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

from shared_models.models import CertificateEnrollment, CourseEnrollment, Cart, LMSAccess
# Django stuff ends

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


def execute(message_data):
    # message_data = {
    #     'data': {
    #         'cid': course_model.external_id
    #     },
    #     'erp': 'mindedge',
    #     'profile': profile,
    #     'action': 'enroll',
    #     'enrollment_type': 'course',
    #     'enrollment_id': str(course_enrollment.id),
    #     'cart_id': str(cart.id)
    # }

    erp = message_data['erp']
    profile = message_data['profile']
    data = message_data['data']
    action = message_data['action']

    erp_config = configs[erp]
    processor_class = erp_config['processor_class']
    credentials = erp_config['credentials']

    processor_obj = processor_class(credentials, profile, data)

    if processor_obj.authenticate():
        status_data = {'comment': 'erp_authenticated', 'data': credentials}
        save_status_history(status_data)
        action = getattr(processor_obj, action)
        resp = action()

        status_data = {'comment': 'enrolled', 'data': resp}
        save_status_history(status_data)

        if message_data['enrollment_type'] == 'course':
            enrollment = CourseEnrollment.objects.get(id=message_data['enrollment_id'])

            status_data = {'comment': 'lms_created'}
            save_status_history(status_data)

            LMSAccess.objects.update_or_create(
                course_enrollment=enrollment,
                defaults={
                    'student_ref': 'student',
                    'lms_access_details': resp,
                }
            )
        else:
            enrollment = CertificateEnrollment.objects.get(id=message_data['enrollment_id'])

        enrollment.enrollment_status = 'enrolled'
        enrollment.save()

        cart = Cart.objects.get(id=message_data['cart_id'])

        cart.cart_status = 'processed'

        cart.save()

    else:
        status_data = {'comment': 'authentication_failed', 'data': credentials}
        save_status_history(status_data)
        resp = {'status': 'fail', 'error': 'Auth failed'}
