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
    get_data,
    es_course_unpublish
)

from .serializers import CourseSerializer, SectionSerializer, CourseModelSerializer,\
    CheckSectionModelValidationSerializer, InstructorModelSerializer, SectionScheduleModelSerializer,\
    PublishLogModelSerializer, ProductSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb
from django_initializer import initialize_django

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product,\
    StoreCourseSection, Store, RelatedProduct
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from models.log.publish_log import PublishLog as PublishLogModel
from datetime import datetime
import decimal
from django.utils import timezone
from processors.publish.create_data import CreateData
from processors.publish.update_data import UpdateData
from processors.publish.deactivate_data import DeactivateData
from processors.publish.store_course_publish import StoreCoursePublish

from django_scopes import scopes_disabled
from models.publish.publish_job import PublishJob as PublishJobModel
from mongoengine import NotUniqueError
from django.db import transaction

initialize_django()


def publish(doc_id):
    doc = PublishJobModel.objects.get(id=doc_id)

    if doc:
        try:
            course_provider_id = doc['course_provider_id']
        except KeyError:
            return False

        try:
            course_provider = CourseProvider.objects.get(id=course_provider_id)
        except CourseProvider.DoesNotExist:
            return False

        try:
            course_provider_model_id = doc['course_provider_model_id']
        except KeyError:
            return False

        try:
            course_provider_model = CourseProviderModel.objects.get(id=course_provider_model_id)
        except CourseProvider.DoesNotExist:
            return False

        contracts = CourseSharingContract.objects.filter(course_provider__id=course_provider_id, is_active=True)

        try:
            action = doc['payload']['action']
            records = doc['payload']['records']
        except KeyError:
            return False
        # create courses first
        # because without courses everything else will not exist

        # later on, when creating sections, instructors or schedules
        # we will assume that their parents exist in db. if does not exist, we will just skip

        if action == "record_add":
            create_data = CreateData()
            publish_course = StoreCoursePublish()
            create_data.create_courses(doc, course_provider, course_provider_model, records, contracts=contracts)
            # since schedules and instructors are embedded into sections, we will create sections first

            for item in records:
                if item['type'] == 'section':
                    create_data.create_sections(doc, item, course_provider, course_provider_model, contracts=contracts)

            # then rest of the stuff
            for item in records:
                if item['type'] == 'schedule':
                    create_data.create_schedules(doc, item, course_provider_model)

                elif item['type'] == 'instructor':
                    create_data.create_instructors(doc, item, course_provider_model)

                elif item['type'] == 'product':
                    create_data.create_products(doc, item, course_provider_model)

                elif item['type'] == 'subject':
                    create_data.create_subjects(doc, item, course_provider, course_provider_model)

                elif item['type'] == 'question':
                    create_data.create_questions(doc, item, course_provider, course_provider_model)

            for item in records:
                if item['type'] == 'course':
                    try:
                        is_published = item['data']['is_published']
                    except KeyError:
                        pass
                    else:
                        publish_course.course_publish_in_stores(doc, item, course_provider, course_provider_model)

        elif action == "record_update":
            update_data = UpdateData()
            publish_course = StoreCoursePublish()
            for item in records:
                if item['type'] == 'course':
                    update_data.update_courses(doc, item, course_provider, course_provider_model, contracts=contracts)

                elif item['type'] == 'section':
                    update_data.update_sections(doc, item, course_provider, course_provider_model, contracts=contracts)

                elif item['type'] == 'schedule':
                    update_data.update_schedules(doc, item, course_provider_model)

                elif item['type'] == 'instructor':
                    update_data.update_instructors(doc, item, course_provider_model)

                elif item['type'] == 'product':
                    update_data.update_products(doc, item, course_provider_model)

                elif item['type'] == 'subject':
                    update_data.update_subjects(doc, item, course_provider, course_provider_model)

                elif item['type'] == 'question':
                    update_data.update_questions(doc, item, course_provider, course_provider_model)

            for item in records:
                if item['type'] == 'course':
                    try:
                        is_published = item['data']['is_published']
                    except KeyError:
                        pass
                    else:
                        publish_course.course_publish_in_stores(doc, item, course_provider, course_provider_model)


        elif action == "record_delete":
            deactivate_data = DeactivateData()
            for item in records:
                if item['type'] == 'course':
                    deactivate_data.deactivate_course(doc, course_provider, course_provider_model, item)

                elif item['type'] == 'section':
                    deactivate_data.deactivate_section(doc, course_provider, course_provider_model, item)

                elif item['type'] == 'schedule':
                    deactivate_data.deactivate_schedule(doc, course_provider, course_provider_model, item)

                elif item['type'] == 'instructor':
                    deactivate_data.deactivate_instructor(doc, course_provider, course_provider_model, item)


        print('message processing complete')
