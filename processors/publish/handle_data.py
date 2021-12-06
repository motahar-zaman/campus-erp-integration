from mongoengine import get_db, connect, disconnect
from bson import ObjectId
from decouple import config
from .helpers import (
    get_datetime_obj,
    upsert_mongo_doc,
    prepare_course_postgres,
    prepare_course_mongo,
    get_execution_site,
    get_instructors,
    get_schedules,
    prepare_section_mongo,
    prepare_section_postgres,
    transale_j1_data
)

from .serializers import CourseSerializer, SectionSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb
from django_initializer import initialize_django
initialize_django()

from shared_models.models import Course, Section, CourseSharingContract, StoreCourse, Product, StoreCourseSection
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from datetime import datetime

from django_scopes import scopes_disabled

def get_data(doc_id, collection):
    db = get_db()
    coll = db[collection]
    doc = coll.find_one({'_id': ObjectId(doc_id)})
    return doc

def create_sections(data, course_provider_id, contracts=[]):
    section_data = data['data']
    try:
        course_model = CourseModel.objects.get(external_id=data['parent'])
    except CourseModel.DoesNotExist:
        # without that we can not proceed comfortably
        return

    with scopes_disabled():
        try:
            course = Course.objects.get(content_db_reference=str(course_model.id), course_provider__id=course_provider_id)
        except Course.DoesNotExist:
            # without that we can not proceed comfortably
            return
        section_data = prepare_section_postgres(section_data, course, course_model)
        try:
            section = course.sections.get(name=section_data['name'])
        except Section.DoesNotExist:
            serializer = SectionSerializer(data=section_data)
        else:
            serializer = SectionSerializer(section, data=section_data)

        if serializer.is_valid(raise_exception=True):
            section = serializer.save()
        print('new section: ', section)
        # now, we find store courses, utilizing contracts.
        # if we find store courses, we update store course sections

        for contract in contracts:
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

                    print('store course section: ', store_course_section, ' product: ', product)
                else:
                    product = store_course_section.product
                    product.store = contract.store
                    product.external_id = course_model.external_id
                    product.product_type = 'section'
                    product.title = course.title
                    product.tax_code = 'ST080031'
                    product.fee = section.fee

                    product.save()

                    print('store course section: ', store_course_section, ' product: ', product)

def create_courses(course_provider_id, course_provider_model_id, records, contracts=[]):
    print('>>> creating courses and store courses')
    for item in records:
        if item['type'] == 'course':
            data = item['data']

            course_model_data = prepare_course_mongo(data, course_provider_model_id)

            course_data = prepare_course_postgres(data, course_provider_id)

            query = {'external_id': course_model_data['external_id'], 'provider': course_model_data['provider']}
            doc_id = upsert_mongo_doc(collection='course', query=query, data=course_model_data)

            course_data['content_db_reference'] = str(doc_id)

            with scopes_disabled():
                try:
                    course = Course.objects.get(slug=course_data['slug'], course_provider__id=course_provider_id)
                except Course.DoesNotExist:
                    course_serializer = CourseSerializer(data=course_data)
                else:
                    course_serializer = CourseSerializer(course, data=course_data)

                if course_serializer.is_valid(raise_exception=True):
                    course = course_serializer.save()

                course_model = CourseModel.objects.get(id=course.content_db_reference)
                print('created course: ', course, course.id)
                # create StoreCourse
                for contract in contracts:
                    store_course, created = StoreCourse.objects.get_or_create(
                        course=course,
                        store=contract.store,
                        defaults={'is_published': True, 'enrollment_ready': True}
                    )
                    print(' created store course: ', store_course, store_course.id)
                    print(' ')

def publish(doc_id):
    doc = get_data(doc_id, collection='publish_job')
    if doc:
        course_provider_id = doc['course_provider_id']
        course_provider_model_id = doc['course_provider_model_id']
        contracts = CourseSharingContract.objects.filter(course_provider__id=course_provider_id, is_active=True)

        try:
            records = doc['payload']['records']
        except KeyError:
            return
        # create courses first
        # because without courses everything else will not exist

        # later on, when creating sections, instructors or schedules
        # we will assume that their parents exist in db. if does not exist, we will just
        print('----------------------------------------')
        create_courses(course_provider_id, course_provider_model_id, records, contracts=contracts)

        for item in records:
            if item['type'] == 'section':
                create_sections(item, course_provider_id, contracts=contracts)
        print('----------------------------------------')

        # now handle everyting else
