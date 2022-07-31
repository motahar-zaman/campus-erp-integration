import json
import time
from formatters.enrollment import EnrollmentFormatter
from formatters.crm import CRMFormatter
from formatters.tax import TaxFormatter
from formatters.importers import ImportFormatter

from processors.enrollment.common import enroll, unenroll
from processors.crm.hubspot import add_or_update_user, add_or_update_product
from processors.tax.avatax import tax_create, tax_refund
from processors.importers.contents import import_courses_mongo, import_courses_postgres, import_sections_mongo,\
    import_sections_postgres, import_profiles_postgres
from processors.publish.handle_data import publish
from processors.notification.notification import notification_to_course_provider

from loggers.elastic_search import upload_log
from shared_models.models import CourseEnrollment, Notification, Event, Payment, Cart

def print_log(data):
    print('------------------------------------')
    print(data)
    print('------------------------------------')

def requestlog_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    upload_log(data)


def enroll_callback(ch, method, properties, body):
    payload = json.loads(body.decode())

    if 'enrollment' in method.routing_key:
        # print_log(payload)
        # print('* Enrolling')
        formatter = EnrollmentFormatter()
        data = formatter.enroll(payload)
        print_log(data)
        # print('enrollment data formatted')
        enroll(data)
        # print('Done')

    if 'crm_user' in method.routing_key:
        # print('Adding/updating user to crm')
        formatter = CRMFormatter()
        data = formatter.add_or_update_user(payload)
        add_or_update_user(data)
        # print('Done')

    if 'crm_product' in method.routing_key:
        # print('Adding/updating product to crm')
        formatter = CRMFormatter()
        data = formatter.add_or_update_product(payload)
        add_or_update_product(data)
        # print('Done')

    if 'tax' in method.routing_key:
        # print('* Adding tax info to avatax')
        formatter = TaxFormatter()
        data = formatter.tax_create(payload)
        tax_create(data)
        # print('Done')


def refund_callback(ch, method, properties, body):
    payload = json.loads(body.decode())

    if 'email' in method.routing_key:
        # print('* Refunding enrollment e.g. sending emails')
        formatter = EnrollmentFormatter()
        data = formatter.unenroll(payload)
        unenroll(data)
        # print('Done')

    if 'crm_product' in method.routing_key:
        # print('* Updating product in crm')
        formatter = CRMFormatter()
        data = formatter.add_or_update_product(payload)
        add_or_update_product(data)
        # print('Done')

    if 'tax' in method.routing_key:
        # print('* Refunding tax')
        formatter = TaxFormatter()
        data = formatter.tax_refund(payload)
        tax_refund(data)
        # print('Done')


def import_callback(ch, method, properties, body):
    payload = json.loads(body.decode())

    if 'course_mongo' in method.routing_key:
        # print('* Importing course to mongo')
        formatter = ImportFormatter()
        import_task = formatter.course(payload)
        import_courses_mongo(import_task)
        # print('Done')

    if 'course_postgres' in method.routing_key:
        # print('* Importing course to postgres')
        formatter = ImportFormatter()
        import_task = formatter.course(payload)
        import_courses_postgres(import_task)
        # print('Done')

    if 'section_mongo' in method.routing_key:
        # print('* Importing section to mongo')
        formatter = ImportFormatter()
        import_task = formatter.section(payload)
        import_sections_mongo(import_task)
        # print('Done')

    if 'section_postgres' in method.routing_key:
        # print('* Importing section to postgres')
        formatter = ImportFormatter()
        import_task = formatter.section(payload)
        import_sections_postgres(import_task)
        print('Done')

    if 'profile_postgres' in method.routing_key:
        print('* Importing profile to postgres')
        formatter = ImportFormatter()
        import_task = formatter.profile(payload)
        import_profiles_postgres(import_task)
        print('Done')


def publish_callback(ch, method, properties, body):
    payload = json.loads(body.decode())
    publish(payload['doc_id'])


def notification_callback(ch, method, properties, body):
    time.sleep(10)  # Sleep for 10 seconds to create cart items
    payload = json.loads(body.decode())
    notification_to_course_provider(payload['notification_id'])
