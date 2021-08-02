import json
import pika
from decouple import config
from bson import ObjectId
import csv
import urllib.request

from django_initializer import initialize_django
initialize_django()

from shared_models.models import Course
from django_scopes import scopes_disabled
from models.course.course import Course as CourseModel


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
    channel.basic_publish(exchange='campusmq', routing_key='course_postgres.import', body=json.dumps({'import_task_id': str(import_task.id)}))
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

    for data in items:
        data = {k.strip(): v.strip() for (k, v) in data.items()}
        data['image'] = {'original': data['image']}
        data['default_image'] = {'original': data['default_image']}
        data['from_importer'] = True
        data['keywords'] = []
        data['provider'] = ObjectId(import_task.course_provider.content_db_reference)
        data['_is_deleted'] = False
        try:
            course_model = CourseModel.with_deleted_objects.get(external_id=data['external_id'], provider=data['provider'])
        except CourseModel.DoesNotExist:
            try:
                course_model = CourseModel.objects.create(**data)
            except Exception as e:
                import_task.status = 'Failed'
                import_task.status_message = data['external_id'] + ': ' + str(e)
                import_task.save()
                break

            else:
                import_task.status = 'Success'
                import_task.queue_processed = 1
                import_task.save()
                import_courses_mongo(import_task)
        else:
            try:
                course_model.update(**data)
            except Exception as e:
                import_task.status = 'Failed'
                import_task.status_message = data['external_id'] + ': ' + str(e)
                import_task.save()
                break

            else:
                import_task.status = 'Success'
                import_task.queue_processed = 1
                import_task.save()
                import_courses_mongo(import_task)


def import_courses_postgres(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)

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
                    course = Course.objects.create(
                        course_provider=import_task.course_provider,
                        title=course_model.title,
                        slug=course_model.slug,
                        content_db_reference=str(course_model.id),
                        course_image_uri=course_model.image['original'],
                        content_ready=False,
                        external_image_url=course_model.default_image,
                    )
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
