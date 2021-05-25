import os
import django
from .service import MindEdgeService
from decouple import config
from status_logger import save_status_to_mongo

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


def enroll(message_data):
    try:
        erp = message_data['erp']
        profile = message_data['profile']
        data = message_data['data']
        action = message_data['action']
    except KeyError:
        save_status_to_mongo({'comment': 'unknown data format'})
        return

    try:
        erp_config = configs[erp]
    except KeyError:
        save_status_to_mongo({'comment': erp + ' not implemented'})
        return
    processor_class = erp_config['processor_class']
    credentials = erp_config['credentials']

    processor_obj = processor_class(credentials, profile, data)

    if processor_obj.authenticate():
        status_data = {'comment': 'erp_authenticated', 'data': credentials}
        save_status_to_mongo(status_data)
        action = getattr(processor_obj, action)
        resp = action()

        status_data = {'comment': 'enrolled', 'data': resp}
        save_status_to_mongo(status_data)

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

            status_data = {'comment': 'lms_created'}
            save_status_to_mongo(status_data)

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
        save_status_to_mongo(status_data)
        resp = {'status': 'fail', 'error': 'Auth failed'}
