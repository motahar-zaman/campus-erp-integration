import json
import pika
from decouple import config
from bson import ObjectId
import csv
import urllib.request
from loggers.mongo import save_status_to_mongo
from config import mongo_client
from django_initializer import initialize_django
from datetime import datetime
from django.utils.formats import get_format
initialize_django()

from shared_models.models import Course, Section
from django_scopes import scopes_disabled
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel


def create_queue_postgres(import_task):
    # create task for importing to postgres
    amqp_user = config('AMQP_USER')
    amqp_pass = config('AMQP_PASS')
    amqp_host = config('AMQP_HOST')
    amqp_port = config('AMQP_PORT')
    amqp_url = f'amqps://{amqp_user}:{amqp_pass}@{amqp_host}:{amqp_port}?connection_attempts=5&retry_delay=5'

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange='campusmq', exchange_type='topic')

    if import_task.import_type == 'course':
        routing_key = 'course_postgres.import'
    if import_task.import_type == 'section':
        routing_key = 'section_postgres.import'

    channel.basic_publish(exchange='campusmq', routing_key=routing_key, body=json.dumps({'import_task_id': str(import_task.id)}))
    print('Published data to MQ, closing connection')
    connection.close()
    print('Done')


def import_courses_mongo(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)
    print('got data from csv file')

    try:
        mongo_client.connect_mongodb()
        for data in items:
            print('importing: ')
            data = {k.strip(): v.strip() for (k, v) in data.items()}
            data['image'] = {'original': data['image']}
            data['default_image'] = {'original': data['default_image']}
            data['from_importer'] = True
            data['keywords'] = []
            data['provider'] = ObjectId(import_task.course_provider.content_db_reference)
            data['_is_deleted'] = False
            try:
                print('getting course document')
                course_model = CourseModel.with_deleted_objects(external_id=data['external_id'], provider=data['provider'])

                raw_query = {'$set': data}
                print('upserting course document')
                course_model.update_one(__raw__=raw_query, upsert=True)
                print('done')
                import_task.status = 'completed'
                import_task.queue_processed = 1
                import_task.save()
                create_queue_postgres(import_task)
            except Exception as e:
                print('execption: ', str(e))
                import_task.status = 'failed'
                msg = {'message': str(e), 'import_task_id': str(import_task.id), 'external_id': data['external_id']}
                save_status_to_mongo(status_data=msg, collection='ImportTaskErrorLog')
                import_task.status_message = data['external_id'] + ': create error'
                import_task.save()

    finally:
        print('operation completed')
        mongo_client.disconnect_mongodb()


def import_courses_postgres(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)

    try:
        mongo_client.connect_mongodb()
        for data in items:
            data = {k.strip(): v.strip() for (k, v) in data.items()}
            data['image'] = {'original': data['image']}
            data['default_image'] = {'original': data['default_image']}
            data['from_importer'] = True
            data['keywords'] = []
            data['provider'] = ObjectId(import_task.course_provider.content_db_reference)

            try:
                course_model = CourseModel.objects.get(external_id=data['external_id'], provider=data['provider'])
            except CourseModel.DoesNotExist:
                pass
            else:
                with scopes_disabled():
                    try:
                        course = Course.objects.get(course_provider=import_task.course_provider, content_db_reference=str(course_model.id))
                    except Course.DoesNotExist:
                        try:
                            course = Course.objects.create(
                                course_provider=import_task.course_provider,
                                title=course_model.title,
                                slug=course_model.slug,
                                content_db_reference=str(course_model.id),
                                course_image_uri=course_model.image['original'],
                                content_ready=False,
                                external_image_url=course_model.default_image,
                            )
                        except Exception as e:
                            msg = {'message': str(e), 'import_task_id': str(import_task.id), 'external_id': data['external_id']}
                            save_status_to_mongo(status_data=msg, collection='ImportTaskErrorLog')
                        import_task.queue_processed = 2
                    else:
                        course.course_provider = import_task.course_provider
                        course.title = course_model.title
                        course.slug = course_model.slug
                        course.content_db_reference = str(course_model.id)
                        course.course_image_uri = course_model.image['original']
                        course.content_ready = False
                        course.external_image_url = course_model.default_image
                        course.save()
                        import_task.queue_processed = 2

        import_task.save()
    finally:
        mongo_client.disconnect_mongodb()


def import_sections_mongo(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)

    try:
        mongo_client.connect_mongodb()
        for data in items:
            data = {k.strip().replace('*', ''): v.strip() for (k, v) in data.items()}

            data['course_fee'] = {'amount': data['course_fee'], 'currency': 'usd'}
            data['instructors'] = []
            try:
                int(data['num_seats'])
            except ValueError:
                data['num_seats'] = 0

            try:
                int(data['available_seats'])
            except ValueError:
                data['available_seats'] = 0

            try:
                float(data['credit_hours'])
            except ValueError:
                data['credit_hours'] = 0.00

            try:
                float(data['ceu_hours'])
            except ValueError:
                data['ceu_hours'] = 0.00

            try:
                float(data['clock_hours'])
            except ValueError:
                data['clock_hours'] = 0.00

            if data['registration_deadline'] == '':
                data['registration_deadline'] = datetime.now().date()

            try:
                float(data['load_hours'])
            except ValueError:
                data['load_hours'] = 0.00

            if data['details_url'] == '':
                data['details_url'] = None

            data['execution_mode'] = str(data['execution_mode']).lower()

            data['provider'] = ObjectId(import_task.course_provider.content_db_reference)
            try:
                course_model = CourseModel.with_deleted_objects.get(external_id=data.pop('course_external_id'), provider=data.pop('provider'))
            except CourseModel.DoesNotExist:
                import_task.queue_processed = 2
                import_task.status = 'failed'
            else:

                if CourseModel.objects(id=course_model.id, sections__code=data['code']).count():
                    print('exists.updating')
                    CourseModel.objects(
                        id=course_model.id, sections__code=data['code']
                    ).update_one(set__sections__S=SectionModel(**data))
                else:
                    print('does not exists. creating')
                    course_model.sections.append(SectionModel(**data))
                    course_model.save()

                import_task.queue_processed = 2

        import_task.save()
    finally:
        mongo_client.disconnect_mongodb()
    create_queue_postgres(import_task)


def parse_date(date_str):
    """Parse date from string by DATE_INPUT_FORMATS of current language"""
    for item in get_format('DATE_INPUT_FORMATS'):
        try:
            date = datetime.strptime(date_str, item).date()
            datetime.combine(date, datetime.max.time())
        except (ValueError, TypeError):
            continue

    return None


def import_sections_postgres(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)

    try:
        mongo_client.connect_mongodb()
        for data in items:
            data = {k.strip().replace('*', ''): v.strip() for (k, v) in data.items()}
            if data['available_seats'] == '':
                data['available_seats'] = None

            if data['num_seats'] == '':
                data['num_seats'] = None
            data['provider'] = ObjectId(import_task.course_provider.content_db_reference)

            try:
                course_model = CourseModel.with_deleted_objects.get(external_id=data.pop('course_external_id'), provider=data.pop('provider'))
            except CourseModel.DoesNotExist:
                pass
            else:
                with scopes_disabled():
                    try:
                        course = Course.objects.get(course_provider=import_task.course_provider, content_db_reference=str(course_model.id))
                    except Course.DoesNotExist:
                        pass
                    else:
                        try:
                            section = Section.objects.get(course=course, name=data['code'])
                        except Section.DoesNotExist:
                            Section.objects.create(
                                course=course,
                                name=data['code'],
                                fee=data['course_fee'],
                                seat_capacity=data['num_seats'],
                                available_seat=data['available_seats'],
                                execution_mode=data['execution_mode'],
                                registration_deadline=parse_date(data['registration_deadline']),
                                content_db_reference=str(course_model.id),
                                is_active=False,
                                start_date=parse_date(data['start_date']),
                                end_date=parse_date(data['end_date']),
                                execution_site=data['execution_site'],
                            )
                        else:
                            section.name = data['code']
                            section.fee = data['course_fee']
                            section.seat_capacity = data['num_seats']
                            section.available_seat = data['available_seats']
                            section.execution_mode = data['execution_mode']
                            section.registration_deadline = data['registration_deadline']
                            section.content_db_reference = str(course_model.id)
                            section.is_active = False
                            section.start_date = data['start_date']
                            section.end_date = data['end_date']
                            section.execution_site = data['execution_site']

                        import_task.queue_processed = 2

        import_task.save()
    finally:
        mongo_client.disconnect_mongodb()
