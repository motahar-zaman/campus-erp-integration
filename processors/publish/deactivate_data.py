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

from django_scopes import scopes_disabled
from models.publish.publish_job import PublishJob as PublishJobModel
from mongoengine import NotUniqueError
from django.db import transaction
from django_initializer import initialize_django

initialize_django()

class DeactivateData():
    def deactivate_course(self, doc, course_provider, course_provider_model, data):
        # insert every item in mongo to get status individually
        mongo_data = {'data': data, 'publish_job_id': doc['id'], 'type': 'course_delete', 'time': timezone.now(),
                      'message': 'task is still in queue', 'status': 'pending',
                      'external_id': data['match']['course']}

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        with scopes_disabled():
            try:
                course_model = CourseModel.objects.get(external_id = data['match']['course'], provider = course_provider.content_db_reference)
            except CourseModel.DoesNotExist:
                inserted_item.errors = {'course_model': ['course_model does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()

                return False

            try:
                course = Course.objects.get(content_db_reference=course_model.id, course_provider=course_provider)
            except Course.DoesNotExist:
                inserted_item.errors = {'course': ['course does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()

                return False

        contracts = CourseSharingContract.objects.filter(course_provider=course_provider, is_active=True)

        with scopes_disabled():
            sections = course.sections.all()
            store_courses = StoreCourse.objects.filter(course=course, store__in=[contract.store for contract in contracts])

        # 4. Get store course sections
        with scopes_disabled():
            store_course_sections = StoreCourseSection.objects.filter(store_course__in=store_courses, section__in=sections)

        #########################################################################################
        # If all the sections of a course are not to be deleted, then the course is not touched.
        # otherwise, the course is deactivated after deactivating all the sections.
        # this works when no section is provide and we consider the whole course (e.g. all the section) for deactivation
        # but will fail when all the sections are provide.
        # in that case, we must manually check if the course has any section left active after the deactivation
        # operation and if there's none, deactivate the course only then

        # 1. Deactivate the Store Course Sections
        with transaction.atomic():
            with scopes_disabled():
                store_course_sections.update(active_status=False)

                # 2. Deactivate the Products associated with the store course_sections here
                Product.objects.filter(
                    id__in=[scs.product.id for scs in store_course_sections]

                ).update(active_status=False)

                # 3. Now the Section
                sections.update(active_status=False)

                # 4. Now the Course if it has no active section
                if course.sections.filter(active_status=True).count() == 0:
                    course.active_status = False
                    course.content_ready = False
                    course.save()
                    # if the course is deactivated, deactivate the store_courses too
                    store_courses.update(active_status=False, enrollment_ready=False)

            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
            inserted_item.save()

        # once the store_course is deactivated, these must be then removed from ES too
        for store_course in store_courses:
            es_course_unpublish(store_course)


    def deactivate_section(self, doc, course_provider, course_provider_model, data):
        # insert every item in mongo to get status individually
        mongo_data = {'data': data, 'publish_job_id': doc['id'], 'type': 'section_delete', 'time': timezone.now(),
                      'message': 'task is still in queue', 'status': 'pending',
                      'external_id': data['match']['course']}

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        with scopes_disabled():
            try:
                course_model = CourseModel.objects.get(external_id = data['match']['course'], provider = course_provider.content_db_reference)
            except CourseModel.DoesNotExist:
                inserted_item.errors = {'course_model': ['course_model does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()

                return False

            section_code = None
            for section in course_model.sections:
                if section.external_id == data['match']['section']:
                    section_code = section['code']
                    break

            try:
                course = Course.objects.get(content_db_reference=course_model.id, course_provider=course_provider)
            except Course.DoesNotExist:
                inserted_item.errors = {'course': ['course does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

            try:
                section = course.sections.get(name=section_code)
            except Section.DoesNotExist:
                inserted_item.errors = {'course': ['section does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
            except Section.MultipleObjectsReturned:
                inserted_item.errors = {'course': ['multiple section found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

        contracts = CourseSharingContract.objects.filter(course_provider=course_provider, is_active=True)

        with scopes_disabled():
            store_courses = StoreCourse.objects.filter(course=course, store__in=[contract.store for contract in contracts])

        # 4. Get store course sections
        with scopes_disabled():
            store_course_sections = StoreCourseSection.objects.filter(store_course__in=store_courses, section=section)

        #########################################################################################
        # If all the sections of a course are not to be deleted, then the course is not touched.
        # otherwise, the course is deactivated after deactivating all the sections.
        # this works when no section is provide and we consider the whole course (e.g. all the section) for deactivation
        # but will fail when all the sections are provide.
        # in that case, we must manually check if the course has any section left active after the deactivation
        # operation and if there's none, deactivate the course only then

        # 1. Deactivate the Store Course Sections
        with transaction.atomic():
            with scopes_disabled():
                store_course_sections.update(active_status=False)

                # 2. Deactivate the Products associated with the store course_sections here
                Product.objects.filter(
                    id__in=[scs.product.id for scs in store_course_sections]

                ).update(active_status=False)

                # 3. Now the Section
                section.active_status=False
                section.save()

                # 4. Now the Course if it has no active section
                if course.sections.filter(active_status=True).count() == 0:
                    course.active_status = False
                    course.content_ready = False
                    course.save()
                    # if the course is deactivated, deactivate the store_courses too
                    store_courses.update(active_status=False, enrollment_ready=False)

            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
            inserted_item.save()

        # once the store_course is deactivated, these must be then removed from ES too
        for store_course in store_courses:
            es_course_unpublish(store_course)


    def deactivate_schedule(self, doc, course_provider, course_provider_model, data):
        # insert every item in mongo to get status individually
        mongo_data = {'data': data, 'publish_job_id': doc['id'], 'type': 'schedule_delete', 'time': timezone.now(),
                      'message': 'task is still in queue', 'status': 'pending',
                      'external_id': data['match']['course']}

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id = data['match']['course'], provider = course_provider.content_db_reference)
        except CourseModel.DoesNotExist:
            inserted_item.errors = {'course_model': ['course_model does not found in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        for section_idx, section in enumerate(course_model.sections):
            if section.external_id == data['match']['section']:
                schedules = section.schedules
                for idx, schedule in enumerate(section.schedules):
                    if schedule.external_id == data['match']['schedule']:
                        schedules.pop(idx)
                        course_model.sections[section_idx].schedules = schedules
                        course_model.save()

                        inserted_item.message = 'task processed successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()
                        return True

        inserted_item.errors = {'course_model': ['matching schedule does not found in database']}
        inserted_item.status = 'failed'
        inserted_item.message = 'error occurred'
        inserted_item.save()
        return False


    def deactivate_instructor(self, doc, course_provider, course_provider_model, data):
        # insert every item in mongo to get status individually
        mongo_data = {'data': data, 'publish_job_id': doc['id'], 'type': 'instructor_delete', 'time': timezone.now(),
                      'message': 'task is still in queue', 'status': 'pending',
                      'external_id': data['match']['course']}

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id = data['match']['course'], provider = course_provider.content_db_reference)
        except CourseModel.DoesNotExist:
            inserted_item.errors = {'course_model': ['course_model does not found in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        try:
            instructor_model = InstructorModel.objects.get(external_id=data['match']['instructor'], provider=course_provider.content_db_reference)
        except InstructorModel.DoesNotExist:
            inserted_item.errors = {'course_model': ['instructor does not found in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False
        except InstructorModel.MultipleObjectsReturned:
            inserted_item.errors = {'course_model': ['multiple instructor exists in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        for section_idx, section in enumerate(course_model.sections):
            if section.external_id == data['match']['section']:
                instructors = section.instructors
                for idx, instructor in enumerate(section.instructors):
                    if instructor == instructor_model:
                        instructors.pop(idx)
                        course_model.sections[section_idx].instructors = instructors
                        course_model.save()

                        inserted_item.message = 'task processed successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()
                        return True

        inserted_item.errors = {'course_model': ['matching instructor does not found in database']}
        inserted_item.status = 'failed'
        inserted_item.message = 'error occurred'
        inserted_item.save()
        return False
