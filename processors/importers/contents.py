import json
import pika
from decouple import config
from bson import ObjectId
from openpyxl import load_workbook
from io import BytesIO
import urllib.request
import pandas as pd
from loggers.mongo import save_status_to_mongo
from config import mongo_client
from django_initializer import initialize_django
from datetime import datetime
from django.utils.formats import get_format
from itertools import groupby
initialize_django()

from shared_models.models import Course, Section
from django_scopes import scopes_disabled
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from models.course.section_schedule import SectionSchedule
from models.occupation.occupation import Occupation as OccupationModel
from models.courseprovider.provider_site import CourseProviderSite
from models.courseprovider.instructor import Instructor


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
    resp_binary = response.read()
    try:
        df = pd.read_excel(resp_binary, sheet_name=import_task.import_type)
    except ValueError:
        print('could not retrieve data. possibly wrong file type.')
        return

    data = df.T.to_dict()
    print('got data from spreadsheet file')

    try:
        mongo_client.connect_mongodb()
        is_success = True
        for key, row in data.items():
            row = {k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()}
            row['image'] = {'original': row['image']}
            row['default_image'] = {'original': row['default_image']}
            row['from_importer'] = True
            row['keywords'] = []
            row['provider'] = ObjectId(import_task.course_provider.content_db_reference)
            row['_is_deleted'] = False
            try:
                print('getting course document')
                course_model = CourseModel.with_deleted_objects(external_id=row['external_id'], provider=row['provider'])

                raw_query = {'$set': row}
                print('upserting course document')
                course_model.update_one(__raw__=raw_query, upsert=True)
                print('done')

            except Exception as e:
                is_success = False
                print('execption: ', str(e))
                import_task.status = 'failed'
                msg = {'message': str(e), 'import_task_id': str(import_task.id), 'external_id': row['external_id']}
                save_status_to_mongo(status_data=msg, collection='ImportTaskErrorLog')
                import_task.status_message = row['external_id'] + ': create error'
                import_task.save()

            try:
                df2 = pd.read_excel(resp_binary, sheet_name='careers')
            except ValueError:
                pass
            else:
                print('got career tagging data')
                career_data = []

                for key, row in df2.T.to_dict().items():
                    career_data.append({k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()})

                for course_external_id, data in groupby(career_data, key=lambda x:x['course_external_id']):
                    soc_codes = [item['soc_code'] for item in list(data)]
                    print('updating course with career data: ', course_external_id, soc_codes)
                    course_model = CourseModel.objects.get(external_id=course_external_id, provider=ObjectId(import_task.course_provider.content_db_reference))
                    course_model.update(
                        pull_all__careers=[item for item in OccupationModel.objects.all()]
                    )
                    course_model.update(add_to_set__careers=[career.id for career in OccupationModel.objects.filter(soc_code__in=soc_codes)])
                    course_model.save()
                    print('career tagging complete')

    finally:
        if is_success:
            import_task.status = 'completed'
            import_task.queue_processed = 1
            import_task.save()
            create_queue_postgres(import_task)

        print('operation completed')
        mongo_client.disconnect_mongodb()


def import_courses_postgres(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    df = pd.read_excel(response.read())
    data = df.T.to_dict()

    try:
        mongo_client.connect_mongodb()
        for key, row in data.items():
            row = {k.strip(): str(v).strip() for (k, v) in row.items()}

            try:
                course_model = CourseModel.objects.get(external_id=row['external_id'], provider=ObjectId(import_task.course_provider.content_db_reference))
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
                            msg = {'message': str(e), 'import_task_id': str(import_task.id), 'external_id': row['external_id']}
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

def get_section_schedules(import_task, row, schedules):
    try:
        schedules = schedules[row['code']]
    except KeyError:
        return []

    return [SectionSchedule(**item) for item in schedules]

def get_section_instructors(import_task, row, instructors):
    try:
        instructors = instructors[row['code']]
    except KeyError:
        return []
    ins = []
    for item in instructors:
        item['provider'] = ObjectId(import_task.course_provider.content_db_reference)
        item['profile_urls'] = {'url': item['profile_urls']}
        item['image'] = {'default': item['image']}
        obj = Instructor.objects.filter(**item).first()
        if not obj:
            obj = Instructor.objects.create(**item)
        ins.append(obj)
    return ins

def import_sections_mongo(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    resp_binary = response.read()
    df = pd.read_excel(resp_binary, sheet_name=import_task.import_type)
    data = df.T.to_dict()

    try:
        mongo_client.connect_mongodb()
        # fix schedules data
        schedules_data = []
        schedules = {}
        try:
            schedules_df = pd.read_excel(resp_binary, 'schedules')
        except ValueError:
            pass
        else:
            for key, row in schedules_df.T.to_dict().items():
                schedules_data.append({k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()})
            for section_code, lst in groupby(schedules_data, key=lambda x:x['section_code']):
                sch_list = []
                for item in list(lst):
                    del item['section_code']
                    sch_list.append(item)
                schedules[section_code] = sch_list

        # fix instructor data
        instructors_data = []
        instructors = {}
        try:
            instructors_df = pd.read_excel(resp_binary, 'instructors')
        except ValueError:
            pass
        else:
            for key, row in instructors_df.T.to_dict().items():
                instructors_data.append({k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()})
            for section_code, lst in groupby(instructors_data, key=lambda x:x['section_code']):
                ins_list = []
                for item in list(lst):
                    del item['section_code']
                    ins_list.append(item)
                instructors[section_code] = ins_list
        # instructor and schedules data done

        for key, row in data.items():
            row = {k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()}

            row['course_fee'] = {'amount': row['course_fee'], 'currency': 'usd'}
            # checking if execution site is present with the provided value. we search with both name and code. if none is found, we create one.
            execution_site = CourseProviderSite.objects.filter(code=row['execution_site'], provider=ObjectId(import_task.course_provider.content_db_reference)).first()

            if not execution_site:
                execution_site = CourseProviderSite.objects.filter(name=row['execution_site'], provider=ObjectId(import_task.course_provider.content_db_reference)).first()

            if not execution_site:
                execution_site = CourseProviderSite.objects.create(
                    name=row['execution_site'],
                    code=row['execution_site'],
                    provider=ObjectId(import_task.course_provider.content_db_reference)
                )
            # execution site done

            try:
                int(row['num_seats'])
            except ValueError:
                row['num_seats'] = 0

            try:
                int(row['available_seats'])
            except ValueError:
                row['available_seats'] = 0

            try:
                float(row['credit_hours'])
            except ValueError:
                row['credit_hours'] = 0.00

            try:
                float(row['ceu_hours'])
            except ValueError:
                row['ceu_hours'] = 0.00

            try:
                float(row['clock_hours'])
            except ValueError:
                row['clock_hours'] = 0.00

            if row['registration_deadline'] == '':
                row['registration_deadline'] = datetime.now().date()

            try:
                float(row['load_hours'])
            except ValueError:
                row['load_hours'] = 0.00

            if row['details_url'] == '':
                row['details_url'] = None

            row['execution_mode'] = str(row['execution_mode']).lower()
            row['execution_site'] = execution_site

            row['provider'] = ObjectId(import_task.course_provider.content_db_reference)

            row['schedules'] = get_section_schedules(import_task, row, schedules)
            row['instructors'] = get_section_instructors(import_task, row, instructors)

            try:
                course_model = CourseModel.with_deleted_objects.get(external_id=row.pop('course_external_id'), provider=row.pop('provider'))
            except CourseModel.DoesNotExist:
                import_task.queue_processed = 2
                import_task.status = 'failed'
            else:

                if CourseModel.objects(id=course_model.id, sections__code=row['code']).count():
                    print('exists.updating')
                    CourseModel.objects(
                        id=course_model.id, sections__code=row['code']
                    ).update_one(set__sections__S=SectionModel(**row))
                else:
                    print('does not exists. creating')
                    course_model.sections.append(SectionModel(**row))
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
    df = pd.read_excel(response.read())
    data = df.T.to_dict()

    try:
        mongo_client.connect_mongodb()
        for key, row in data.items():
            row = {k.strip().replace('*', ''): str(v).strip() for (k, v) in row.items()}
            if row['available_seats'] == '':
                row['available_seats'] = None

            if row['num_seats'] == '':
                row['num_seats'] = None
            row['provider'] = ObjectId(import_task.course_provider.content_db_reference)

            try:
                course_model = CourseModel.with_deleted_objects.get(external_id=row.pop('course_external_id'), provider=row.pop('provider'))
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
                            section = Section.objects.get(course=course, name=row['code'])
                        except Section.DoesNotExist:
                            Section.objects.create(
                                course=course,
                                name=row['code'],
                                fee=row['course_fee'],
                                seat_capacity=row['num_seats'],
                                available_seat=row['available_seats'],
                                execution_mode=row['execution_mode'],
                                registration_deadline=parse_date(row['registration_deadline']),
                                content_db_reference=str(course_model.id),
                                is_active=False,
                                start_date=parse_date(row['start_date']),
                                end_date=parse_date(row['end_date']),
                                execution_site=row['execution_site'],
                            )
                        else:
                            section.name = row['code']
                            section.fee = row['course_fee']
                            section.seat_capacity = row['num_seats']
                            section.available_seat = row['available_seats']
                            section.execution_mode = row['execution_mode']
                            section.registration_deadline = row['registration_deadline']
                            section.content_db_reference = str(course_model.id)
                            section.is_active = False
                            section.start_date = row['start_date']
                            section.end_date = row['end_date']
                            section.execution_site = row['execution_site']

                        import_task.queue_processed = 2

        import_task.save()
    finally:
        mongo_client.disconnect_mongodb()
