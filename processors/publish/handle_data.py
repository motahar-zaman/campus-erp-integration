from mongoengine import get_db, connect, disconnect
from bson import ObjectId
from decouple import config
from .helpers import (
    get_datetime_obj,
    prepare_course_postgres,
    get_execution_site,
    get_instructors,
    get_schedules,
    prepare_section_mongo,
    prepare_section_postgres,
    transale_j1_data,
    write_status,
    write_log,
    insert_into_mongo,
    get_data
)

from .serializers import CourseSerializer, SectionSerializer, CourseModelSerializer, CheckSectionModelValidationSerializer, InstructorModelSerializer, SectionScheduleModelSerializer, PublishLogModelSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb
from django_initializer import initialize_django
initialize_django()

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product, StoreCourseSection
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from models.log.publish_log import PublishLog as PublishLogModel
from datetime import datetime
import decimal
from django.utils import timezone

from django_scopes import scopes_disabled
from models.publish.publish_job import PublishJob as PublishJobModel

def create_sections(doc, data, course_provider, course_provider_model, contracts=[]):
    # insert every item in mongo to get status individually
    entity_id = data['data']['external_id']
    mongo_data = {'data': data, 'publish_job_id': doc['id'], 'publish_type': 'section', 'entity_id': entity_id,
                  'status': 'initiated', 'logs': [{'level': 'info', 'message': 'initiated', 'time': datetime.now()}]}
    log_serializer = PublishLogModelSerializer(data=mongo_data)
    log = None
    if log_serializer.is_valid():
        log = log_serializer.save()
    else:
        print(log_serializer.errors)

    inserted_item = PublishLogModel.objects.get(id=log.id)

    try:
        course_model = CourseModel.objects.get(external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        inserted_item.logs.append({'level': 'critical', 'message': 'invalid parent in section', 'time': datetime.now()})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False

    try:
        data['data']['start_date'] = get_datetime_obj(data['data']['start_date'])
    except KeyError:
        data['data']['start_date'] = None

    try:
        data['data']['end_date'] = get_datetime_obj(data['data']['end_date'])
    except KeyError:
        data['data']['end_date'] = None

    try:
        data['data']['registration_deadline'] = get_datetime_obj(data['data']['registration_deadline'])
    except KeyError:
        data['data']['registration_deadline'] = None

    data['data']['course_fee'] = {'amount': data['data'].get('fee', ''), 'currency': 'USD'}

    with scopes_disabled():
        try:
            course = Course.objects.get(content_db_reference=str(course_model.id), course_provider=course_provider)
        except Course.DoesNotExist:
            # without that we can not proceed comfortably
            # write_log(inserted_item, 'postgres does not have a corresponding course', 'queue_item')
            # write_status(inserted_item, 'failed', collection='queue_item')
            inserted_item.logs.append({'level': 'critical', 'message':  'postgres does not have a corresponding course', 'time': datetime.now()})
            inserted_item.status = 'failed'
            inserted_item.save()
            return False
    # now update the sections in mongo

    section_model_serializer = CheckSectionModelValidationSerializer(data=data['data'])
    if section_model_serializer.is_valid():
        pass
    else:
        # write_log(inserted_item, section_model_serializer.errors, 'queue_item')
        # write_status(inserted_item, 'failed', collection='queue_item')
        inserted_item.logs.append({'level': 'critical', 'message': section_model_serializer.errors, 'time': datetime.now()})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False
    if course_model.sections:
        for section_idx, sec_data in enumerate(course_model.sections):
            if sec_data['external_id'] == section_model_serializer.data['external_id']:
                new_section_data = sec_data.to_mongo().to_dict()
                new_section_data.update(section_model_serializer.data)
                course_model.sections[section_idx] = SectionModel(**new_section_data)
                course_model.save()
                break
        else:
            CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=section_model_serializer.data)
    else:
        CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=section_model_serializer.data)
    course_model.reload()
    section_data = prepare_section_postgres(section_model_serializer.data, data['data'].get('fee', '0.00'),  course, course_model)
    with scopes_disabled():
        try:
            section = course.sections.get(name=section_data['name'])
        except Section.DoesNotExist:
            serializer = SectionSerializer(data=section_data)
        else:
            serializer = SectionSerializer(section, data=section_data)

        if serializer.is_valid():
            section = serializer.save()
            # write_log(inserted_item, 'section created', 'queue_item')
            # write_status(inserted_item, 'successful', collection='queue_item')
            inserted_item.logs.append({'level': 'info', 'message': 'section created', 'time': datetime.now()})
            inserted_item.status = 'successful'
            inserted_item.save()
        else:
            # write_log(inserted_item, serializer.errors, 'queue_item')
            # write_status(inserted_item, 'failed', collection='queue_item')
            inserted_item.logs.append({'level': 'critical', 'message': 'serializer.errors', 'time': datetime.now()})
            inserted_item.status = 'failed'
            inserted_item.save()
            return False

    # now, we find store courses, utilizing contracts.
    # if we find store courses, we update store course sections

    for contract in contracts:
        with scopes_disabled():
            try:
                store_course = StoreCourse.objects.get(store=contract.store, course=course)
            except StoreCourse.DoesNotExist:
                pass
            except StoreCourse.MultipleObjectsReturned:
                pass
            else:
                try:
                    store_course_section = StoreCourseSection.objects.get(store_course=store_course, section=section)
                except StoreCourseSection.DoesNotExist:
                    # create product
                    product = Product.objects.create(
                        store=contract.store,
                        external_id=course_model.external_id,
                        product_type='section',
                        title=course.title,
                        tax_code='ST080031',
                        fee=section.fee,
                        product_fee=section.fee
                    )

                    store_course_section, created = StoreCourseSection.objects.get_or_create(
                        store_course=store_course,
                        section=section,
                        is_published=False,
                        product=product
                    )

                else:
                    product = store_course_section.product
                    product.store = contract.store
                    product.external_id = course_model.external_id
                    product.product_type = 'section'
                    product.title = course.title
                    product.tax_code = 'ST080031'
                    product.fee = section.fee
                    product.product_fee = section.fee

                    product.save()
    # write_log(inserted_item, 'section and product created', 'queue_item')
    inserted_item.logs.append({'level': 'info', 'message': 'section and product created', 'time': datetime.now()})
    inserted_item.save()


def create_schedules(doc, data, course_provider_model):
    # insert every item in mongo to get status individually
    entity_id = data['data']['external_id']
    mongo_data = {'data': data, 'publish_job_id': doc['id'], 'publish_type': 'schedule', 'entity_id': entity_id,
                  'status': 'initiated', 'logs': [{'level': 'info', 'message': 'initiated', 'time': timezone.now()}]}
    log_serializer = PublishLogModelSerializer(data=mongo_data)
    log = None
    if log_serializer.is_valid():
        log = log_serializer.save()
    else:
        print(log_serializer.errors)

    inserted_item = PublishLogModel.objects.get(id=log.id)

    try:
        course_model = CourseModel.objects.get(sections__external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        inserted_item.logs.append({'level': 'critical', 'message': 'invalid parent in schedule', 'time': timezone.now()})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False

    except CourseModel.MultipleObjectsReturned:
        # without that we can not proceed comfortably
        inserted_item.logs.append({'level': 'critical', 'message': 'many sections with the same external_id', 'time': timezone.now()})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False

    try:
        data['data']['start_at'] = get_datetime_obj(data['data']['start_at'])
    except KeyError:
        data['data']['start_at'] = None

    try:
        data['data']['end_at'] = get_datetime_obj(data['data']['end_at'])
    except KeyError:
        data['data']['end_at'] = None

    # check if the provided data is valid. if not, abort.
    schedule_serializer = SectionScheduleModelSerializer(data=data['data'])
    if not schedule_serializer.is_valid():

        inserted_item.logs.append({'level': 'critical', 'message': schedule_serializer.errors, 'time': timezone.now()})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False

    for section_idx, section in enumerate(course_model.sections):
        if section['external_id'] == data['parent']:
            serializer = CheckSectionModelValidationSerializer(section)
            if serializer.data['schedules']:
                for schedule_idx, schedule in enumerate(serializer.data['schedules']):
                    if schedule['external_id'] == data['data']['external_id']:
                        serializer.data['schedules'][schedule_idx].update(schedule_serializer.data)
                    else:
                        serializer.data['schedules'].append(schedule_serializer.data)
            else:
                serializer.data['schedules'].append(schedule_serializer.data)

            course_model.sections[section_idx] = SectionModel(**serializer.data)
            course_model.save()
            inserted_item.logs.append({'level': 'critical', 'message': 'schedule created', 'time': timezone.now()})
            inserted_item.status = 'successful'
            inserted_item.save()
            break


def create_instructors(doc, data, course_provider_model):
    # insert every item in mongo to get status individually
    entity_id = data['data']['external_id']
    mongo_data = {'data': data, 'publish_job_id': doc['id'], 'publish_type': 'instructor', 'entity_id': entity_id,
                  'status': 'initiated', 'logs': [{'level': 'info', 'message': 'initiated'}]}
    log_serializer = PublishLogModelSerializer(data=mongo_data)
    log = None
    if log_serializer.is_valid():
        log = log_serializer.save()
    else:
        print(log_serializer.errors)

    inserted_item = PublishLogModel.objects.get(id=log.id)

    try:
        course_model = CourseModel.objects.get(sections__external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        inserted_item.logs.append({'level': 'critical', 'message': 'invalid parent in instructor'})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False
    data['data']['provider'] = course_provider_model.id
    try:
        instructor_model = InstructorModel.objects.get(external_id=data['data']['external_id'], provider=course_provider_model)
    except InstructorModel.DoesNotExist:
        instructor_model_serializer = InstructorModelSerializer(data=data['data'])
    else:
        instructor_model_serializer = InstructorModelSerializer(instructor_model, data=data['data'])

    if instructor_model_serializer.is_valid():
        instructor_model = instructor_model_serializer.save()
        inserted_item.logs.append({'level': 'info', 'message': 'instructor updated'})
        inserted_item.status = 'successful'
        inserted_item.save()
    else:
        inserted_item.logs.append({'level': 'critical', 'message': instructor_model_serializer.errors})
        inserted_item.status = 'failed'
        inserted_item.save()
        return False

    for section in course_model.sections:
        if section['external_id'] == data['parent']:
            if instructor_model.id not in section['instructors']:
                serializer = CheckSectionModelValidationSerializer(section)
                serializer.data['instructors'].append(instructor_model.id)
                CourseModel.objects(
                    id=course_model.id,
                    sections__external_id=data['parent'],
                ).update_one(set__sections__S=SectionModel(**serializer.data))

    inserted_item.logs.append({'level': 'info', 'message': 'instructor created and updated'})
    inserted_item.save()
    return True


def create_courses(doc, course_provider, course_provider_model, records, contracts=[]):
    for item in records:
        if item['type'] == 'course':
            # insert every item in mongo to get status individually
            entity_id = item['data']['external_id']
            mongo_data = {'data': item, 'publish_job_id': doc['id'], 'publish_type': 'course', 'entity_id': entity_id,
                          'status': 'initiated', 'logs': [{'level': 'info', 'message': 'initiated', 'time': timezone.now()}]}

            log_serializer = PublishLogModelSerializer(data=mongo_data)
            if log_serializer.is_valid():
                log = log_serializer.save()
            else:
                print(log_serializer.errors)

            inserted_item = PublishLogModel.objects.get(id=log.id)

            data = item['data']
            level = data.get('level', None)
            if level not in ['beginner', 'intermediate', 'advanced']:
                level = ''

            data['level'] = level
            data['provider'] = course_provider_model.id
            try:
                course_model = CourseModel.objects.get(external_id=data['external_id'], provider=course_provider_model)
            except CourseModel.DoesNotExist:
                course_model_serializer = CourseModelSerializer(data=data)
            else:
                course_model_serializer = CourseModelSerializer(course_model, data=data)

            if course_model_serializer.is_valid():
                course_model = course_model_serializer.save()
            else:
                inserted_item.logs.append({'level': 'critical', 'message': course_model_serializer.errors, 'time': timezone.now()})
                inserted_item.status = 'failed'
                inserted_item.save()

                return False

            inserted_item.logs.append({'level': 'info', 'message':
                'course saved in mongodb. trying to get data formatted for postgres', 'time': timezone.now()})

            course_data = prepare_course_postgres(course_model, course_provider)

            inserted_item.logs.append({'level': 'info', 'message': 'prepare course for postgres', 'time': timezone.now()})
            inserted_item.save()

            with scopes_disabled():
                try:
                    course = Course.objects.get(slug=course_data['slug'], course_provider=course_provider)
                except Course.DoesNotExist:
                    course_serializer = CourseSerializer(data=course_data)
                else:
                    course_serializer = CourseSerializer(course, data=course_data)

                if course_serializer.is_valid():
                    course = course_serializer.save()
                    inserted_item.logs.append({'level': 'info', 'message': 'course created in postgres', 'time': timezone.now()})
                    inserted_item.status = 'successful'
                    inserted_item.save()
                    # write_log(inserted_item, 'course created in postgres', 'queue_item')
                    # write_status(inserted_item, 'successful', collection='queue_item')
                else:
                    inserted_item.logs.append({'level': 'critical', 'message': course_serializer.errors, 'time': timezone.now()})
                    inserted_item.status = 'failed'
                    inserted_item.save()

                    return False

                # create StoreCourse
                for contract in contracts:
                    store_course, created = StoreCourse.objects.get_or_create(
                        course=course,
                        store=contract.store,
                        defaults={'enrollment_ready': True, 'is_featured': False, 'is_published': False}
                    )
                inserted_item.logs.append({'level': 'info', 'message': 'course and store_course created in postgres', 'time': timezone.now()})
                inserted_item.save()

    return True


def publish(doc_id):
    doc = PublishJobModel.objects.get(id=doc_id)

    if doc:
        # write_status(doc, 'request received', collection='publish_job')
        try:
            course_provider_id = doc['course_provider_id']
        except KeyError:
            # write_status(doc, 'failed', collection='publish_job')
            return False

        try:
            course_provider = CourseProvider.objects.get(id=course_provider_id)
        except CourseProvider.DoesNotExist:
            # write_status(doc, 'failed', collection='publish_job')
            return False

        try:
            course_provider_model_id = doc['course_provider_model_id']
        except KeyError:
            # write_status(doc, 'failed', collection='publish_job')
            return False

        try:
            course_provider_model = CourseProviderModel.objects.get(id=course_provider_model_id)
        except CourseProvider.DoesNotExist:
            # write_status(doc, 'failed', collection='publish_job')
            return False

        contracts = CourseSharingContract.objects.filter(course_provider__id=course_provider_id, is_active=True)
        # if contracts.count() > 0:
        #     write_log(doc, 'contracts found', 'publish_job')
        try:
            records = doc['payload']['records']
        except KeyError:
            # write_status(doc, 'payload does not contain any records', collection='publish_job')
            return False
        # create courses first
        # because without courses everything else will not exist

        # later on, when creating sections, instructors or schedules
        # we will assume that their parents exist in db. if does not exist, we will just skip
        create_courses(doc, course_provider, course_provider_model, records, contracts=contracts)
        # since schedules and instructors are embedded into sections, we will create sections first

        for item in records:
            if item['type'] == 'section':
                create_sections(doc, item, course_provider, course_provider_model, contracts=contracts)
        # write_log(doc, 'section creation is completed', 'publish_job')

        # then rest of the stuff
        for item in records:
            if item['type'] == 'schedule':
                create_schedules(doc, item, course_provider_model)
            if item['type'] == 'instructor':
                create_instructors(doc, item, course_provider_model)

        print('message processing complete')
        # write_log(doc, 'message processing complete', 'publish_job')
        # write_status(doc, 'successful', collection='publish_job')
        # now handle everyting else
