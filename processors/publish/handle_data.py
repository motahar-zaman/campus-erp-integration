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
    write_status
)

from .serializers import CourseSerializer, SectionSerializer, CourseModelSerializer, CheckSectionModelValidationSerializer, InstructorModelSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb
from django_initializer import initialize_django
initialize_django()

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product, StoreCourseSection
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from datetime import datetime
import decimal

from django_scopes import scopes_disabled

def get_data(doc_id, collection):
    db = get_db()
    coll = db[collection]
    doc = coll.find_one({'_id': ObjectId(doc_id)})
    return doc

def create_sections(doc, data, course_provider, course_provider_model, contracts=[]):
    try:
        course_model = CourseModel.objects.get(external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        write_status(doc, 'invalid parent in section')
        return

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
            write_status(doc, 'postgres does not have a corresponding course')
            return
    # now update the sections in mongo

    section_model_serializer = CheckSectionModelValidationSerializer(data=data['data'])
    if section_model_serializer.is_valid():
        pass
    else:
        write_status(doc, section_model_serializer.errors)
        return

    for sec_data in course_model.sections:
        if sec_data['external_id'] == section_model_serializer.data['external_id']:
            CourseModel.objects(id=course_model.id).update_one(pull__sections__external_id=sec_data['external_id'])
            new_section_data = sec_data.to_mongo().to_dict()
            new_section_data.update(section_model_serializer.data)
        else:
            new_section_data = section_model_serializer.data
        CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=new_section_data)
    if not course_model.sections:
        CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=section_model_serializer.data)

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
        else:
            write_status(doc, serializer.errors)
            return

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
                        fee=section.fee
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

                    product.save()

def create_schedules(doc, data, course_provider_model):
    try:
        course_model = CourseModel.objects.get(sections__external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        write_status(doc, 'invalid parent in schedule')
        return

    except CourseModel.MultipleObjectsReturned:
        # without that we can not proceed comfortably
        write_status(doc, 'many sections with the same external_id')
        return

    try:
        data['data']['start_at'] = get_datetime_obj(data['data']['start_at'])
    except KeyError:
        data['data']['start_at'] = None

    try:
        data['data']['end_at'] = get_datetime_obj(data['data']['end_at'])
    except KeyError:
        data['data']['end_at'] = None

    for section in course_model.sections:
        if section['external_id'] == data['parent']:
            serializer = CheckSectionModelValidationSerializer(section)
            for schedule in section['schedules']:
                if schedule['name'] == data['data']['name']:
                    pass
                else:
                    serializer.data['schedules'].append(data['data'])

            CourseModel.objects(
                id=course_model.id,
                sections__code=section['code'],
            ).update_one(set__sections__S=SectionModel(**serializer.data))

def create_instructors(doc, data, course_provider_model):
    try:
        course_model = CourseModel.objects.get(sections__external_id=data['parent'], provider=course_provider_model)
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        write_status(doc, 'invalid parent in instructor')
        return
    data['data']['provider'] = course_provider_model.id
    try:
        instructor_model = InstructorModel.objects.get(external_id=data['data']['external_id'], provider=course_provider_model)
    except InstructorModel.DoesNotExist:
        instructor_model_serializer = InstructorModelSerializer(data=data['data'])
    else:
        instructor_model_serializer = InstructorModelSerializer(instructor_model, data=data['data'])

    if instructor_model_serializer.is_valid():
        instructor_model = instructor_model_serializer.save()
    else:
        write_status(doc, instructor_model_serializer.errors)
        return

    for section in course_model.sections:
        if section['external_id'] == data['parent']:
            # CourseModel.objects(id=course_model.id).update_one(pull__sections__external_id=section['external_id'])
            # new_section_data = section.to_mongo().to_dict()

            # if instructor_model.id in new_section_data['instructors']:
            #     pass
            # else:
            #     new_section_data['instructors'].append(instructor_model.id)
            # CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=new_section_data)
            # break

            if instructor_model.id not in section['instructors']:
                serializer = CheckSectionModelValidationSerializer(section)
                serializer.data['instructors'].append(instructor_model.id)
                CourseModel.objects(
                    id=course_model.id,
                    sections__external_id=data['parent'],
                ).update_one(set__sections__S=SectionModel(**serializer.data))

def create_courses(doc, course_provider, course_provider_model, records, contracts=[]):
    for item in records:
        if item['type'] == 'course':
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
                write_status(doc, course_model_serializer.errors)
                return

            course_data = prepare_course_postgres(course_model, course_provider)

            with scopes_disabled():
                try:
                    course = Course.objects.get(slug=course_data['slug'], course_provider=course_provider)
                except Course.DoesNotExist:
                    course_serializer = CourseSerializer(data=course_data)
                else:
                    course_serializer = CourseSerializer(course, data=course_data)

                if course_serializer.is_valid():
                    course = course_serializer.save()
                else:
                    write_status(doc, course_serializer.errors)
                    return

                # create StoreCourse
                for contract in contracts:
                    store_course, created = StoreCourse.objects.get_or_create(
                        course=course,
                        store=contract.store,
                        defaults={'is_published': True, 'enrollment_ready': True}
                    )

def publish(doc_id):
    doc = get_data(doc_id, collection='publish_job')
    if doc:
        try:
            course_provider_id = doc['course_provider_id']
        except KeyError:
            write_status(doc, 'key course_provider_id does not exist in data')
            return

        try:
            course_provider = CourseProvider.objects.get(id=course_provider_id)
        except CourseProvider.DoesNotExist:
            write_status(doc, 'course provider not found')
            return

        try:
            course_provider_model_id = doc['course_provider_model_id']
        except KeyError:
            write_status(doc, 'key course_provider_model_id does not exist in data')
            return

        try:
            course_provider_model = CourseProviderModel.objects.get(id=course_provider_model_id)
        except CourseProvider.DoesNotExist:
            write_status(doc, 'course provider model not found')
            return

        contracts = CourseSharingContract.objects.filter(course_provider__id=course_provider_id, is_active=True)

        try:
            records = doc['payload']['records']
        except KeyError:
            return
        # create courses first
        # because without courses everything else will not exist

        # later on, when creating sections, instructors or schedules
        # we will assume that their parents exist in db. if does not exist, we will just skip
        create_courses(doc, course_provider, course_provider_model, records, contracts=contracts)

        for item in records:
            if item['type'] == 'section':
                create_sections(doc, item, course_provider, course_provider_model, contracts=contracts)

            if item['type'] == 'schedule':
                create_schedules(doc, item, course_provider_model)
            if item['type'] == 'instructor':
                create_instructors(doc, item, course_provider_model)
        print('message processing complete')
        # now handle everyting else