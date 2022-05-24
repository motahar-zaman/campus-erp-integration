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
    PublishLogModelSerializer, ProductSerializer, CatalogSerializer, CourseCatalogSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product,\
    StoreCourseSection, Store, RelatedProduct, Catalog, CourseCatalog
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
from django.utils.text import slugify
from django_initializer import initialize_django

initialize_django()


class CreateData():
    def create_courses(self, doc, course_provider, course_provider_model, records, contracts=[]):
        for item in records:
            if item['type'] == 'course':
                # insert every item in mongo to get status individually
                mongo_data = {
                    'data': item, 'publish_job_id': doc['id'], 'type': 'course_create', 'time': timezone.now(),
                    'message':'task is still in queue', 'status': 'pending',
                    'external_id': item['data'].get('external_id', '')
                }

                log_serializer = PublishLogModelSerializer(data=mongo_data)

                if log_serializer.is_valid():
                    inserted_item = log_serializer.save()
                else:
                    print(log_serializer.errors)

                data = item['data']
                level = data.get('level', None)
                if level not in ['beginner', 'intermediate', 'advanced']:
                    level = ''

                data['level'] = level
                data['provider'] = course_provider_model.id
                course_model_serializer = CourseModelSerializer(data=data)

                if course_model_serializer.is_valid():
                    try:
                        course_model = course_model_serializer.save()
                    except NotUniqueError:
                        inserted_item.errors = {'title': ['Tried to save duplicate unique key slug from title']}
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        continue

                else:
                    inserted_item.errors = course_model_serializer.errors
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    continue

                course_data = prepare_course_postgres(course_model, course_provider)

                with scopes_disabled():
                    course_serializer = CourseSerializer(data=course_data)

                    if course_serializer.is_valid():
                        course = course_serializer.save()
                        inserted_item.message = 'task processed successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()

                    else:
                        inserted_item.errors = course_serializer.errors
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        continue

                    # create StoreCourse
                    for contract in contracts:
                        store_course, created = StoreCourse.objects.get_or_create(
                            course=course,
                            store=contract.store,
                            defaults={'enrollment_ready': True, 'is_featured': False, 'is_published': False}
                        )

        return True


    def create_sections(self, doc, data, course_provider, course_provider_model, contracts=[]):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'section_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id=data['parent']['course'], provider=course_provider_model)
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['invalid parent in section']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        data['data']['course_fee'] = {'amount': data['data'].get('fee', ''), 'currency': 'USD'}

        with scopes_disabled():
            try:
                course = Course.objects.get(content_db_reference=str(course_model.id), course_provider=course_provider)
            except Course.DoesNotExist:
                # without that we can not proceed comfortably
                inserted_item.errors = {'parent': ['corresponding course does not found in database']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
        # now update the sections in mongo

        section_model_serializer = CheckSectionModelValidationSerializer(data=data['data'])
        if section_model_serializer.is_valid():
            pass
        else:
            inserted_item.errors = section_model_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
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
                inserted_item.message = 'task processed successfully'
                inserted_item.status = 'completed'
                inserted_item.save()
            else:
                inserted_item.errors = serializer.errors
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

        # now, we find store courses, utilizing contracts.
        # if we find store courses, we update store course sections

        related_products = data.get('related_records', [])

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
                            minimum_fee=section.fee
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
                        product.minimum_fee = section.fee
                        product.save()

                    for related_product in related_products:
                        try:
                            child_product = Product.objects.get(
                                external_id=related_product['external_id'],
                                product_type=related_product['type']
                            )
                        except Exception:
                            pass
                        else:
                            try:
                                related_product = RelatedProduct.objects.create(
                                    product=product,
                                    related_product=child_product,
                                    related_product_type=related_product['relation_type']
                                )
                            except Exception:
                                pass


    def create_schedules(self, doc, data, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'schedule_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id=data['parent']['course'], sections__external_id=data['parent']['section'], provider=course_provider_model)
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['invalid parent in schedule']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        except CourseModel.MultipleObjectsReturned:
            # without that we can not proceed comfortably
            inserted_item.errors = {'external_id': ['many sections with the same external_id']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        try:
            data['data']['start_at'] = get_datetime_obj(data['data']['start_at'], inserted_item=inserted_item)
            if not data['data']['start_at']:
                inserted_item.errors = {'start_at': ['invalid date']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
        except KeyError:
            data['data']['start_at'] = None

        try:
            data['data']['end_at'] = get_datetime_obj(data['data']['end_at'], inserted_item=inserted_item)
            if not data['data']['end_at']:
                inserted_item.errors = {'end_at': ['invalid date']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
        except KeyError:
            data['data']['end_at'] = None

        # check if the provided data is valid. if not, abort.
        schedule_serializer = SectionScheduleModelSerializer(data=data['data'])
        if not schedule_serializer.is_valid():
            inserted_item.errors = schedule_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        for section_idx, section in enumerate(course_model.sections):
            # schedule_exist = False
            if section['external_id'] == data['parent']['section']:
                serializer = CheckSectionModelValidationSerializer(section)
                serializer.data['schedules'].append(schedule_serializer.data)

                course_model.sections[section_idx] = SectionModel(**serializer.data)
                course_model.save()
                inserted_item.message = 'task processed successfully'
                inserted_item.status = 'completed'
                inserted_item.save()
                break


    def create_instructors(self, doc, data, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'instructor_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id=data['parent']['course'], sections__external_id=data['parent']['section'], provider=course_provider_model)
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['invalid parent in instructor']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        data['data']['provider'] = course_provider_model.id
        instructor_model_serializer = InstructorModelSerializer(data=data['data'])

        if instructor_model_serializer.is_valid():
            instructor_model = instructor_model_serializer.save()
            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
            inserted_item.save()

        else:
            inserted_item.errors = instructor_model_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        for section in course_model.sections:
            if section['external_id'] == data['parent']['section']:
                serializer = CheckSectionModelValidationSerializer(section)
                serializer.data['instructors'].append(instructor_model.id)
                CourseModel.objects(
                    id=course_model.id,
                    external_id=data['parent']['course'],
                    sections__external_id=data['parent']['section'],
                ).update_one(set__sections__S=SectionModel(**serializer.data))

        return True


    def create_products(self, doc, item, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'product_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            store = Store.objects.get(url_slug=item['data']['store_slug'])
        except Store.DoesNotExist:
            inserted_item.errors = {'store': ['corresponding store does not found in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        else:
            # create or update product
            data = item['data']
            product_data = {
                'store': store.id,
                'external_id': data.get('external_id', None),
                'product_type': data.get('product_type', ''),
                'title': data.get('title', ''),
                'content': data.get('content', {}),
                'limit_applicable': data.get('limit_applicable', False),
                'total_quantity': data.get('total_quantity', None),
                'quantity_sold': data.get('quantity_sold', 0),
                'available_quantity': data.get('available_quantity', None),
                'tax_code': data.get('tax_code', ''),
                'fee': data.get('fee', 0),
                'minimum_fee': data.get('minimum_fee', 0),
                'currency_code': data.get('currency_code', 'usd')
            }

            with scopes_disabled():
                try:
                    product = Product.objects.get(external_id= str(item['data']['external_id']), store=store, product_type=item['data']['product_type'])
                except Product.DoesNotExist:
                    product_serializer = ProductSerializer(data=product_data)
                else:
                    product_serializer = ProductSerializer(product, data=product_data)

                if product_serializer.is_valid():
                    product = product_serializer.save()
                    print(product.ref_id)
                    inserted_item.message = 'task processed successfully'
                    inserted_item.status = 'completed'
                else:
                    inserted_item.errors = product_serializer.errors
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                inserted_item.save()

        return True


    def create_and_update_subjects(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'subject_create_or_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        data = item['data']

        courses = []
        #getting course from given course external_id
        for tagging_course in item['related_records']:
            if tagging_course.get('type', '') == 'course':
                try:
                    course_model = CourseModel.objects.get(
                        external_id=tagging_course.get('external_id', ''),
                        provider=course_provider_model
                    )
                except CourseModel.DoesNotExist:
                    pass
                else:
                    with scopes_disabled():
                        try:
                            course = Course.objects.get(
                                content_db_reference=str(course_model.id),
                                course_provider=course_provider
                            )
                        except Course.DoesNotExist:
                            pass
                        else:
                            courses.append(course)

        # getting store from given store_slug list
        # create catalog for that store
        # tag catalog with store course

        for store_slug in item['publishing_stores']:
            try:
                store = Store.objects.get(url_slug=store_slug)
            except Store.DoesNotExist:
                inserted_item.errors[store_slug] = ['corresponding store does not found in database']
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                continue
            else:
                data['store'] = str(store.id)
                data['slug'] = slugify(data['title'])

            with scopes_disabled():
                try:
                    catalog = Catalog.objects.get(external_id= str(data['external_id']), store=data['store'], slug=data['slug'])
                except Catalog.DoesNotExist:
                    catalog_serializer = CatalogSerializer(data=data)
                else:
                    catalog_serializer = CatalogSerializer(catalog, data=data)

                if catalog_serializer.is_valid():
                    catalog = catalog_serializer.save()
                    inserted_item.errors[store_slug] = ['subject created successfully']
                    inserted_item.save()

                    for course in courses:
                        try:
                            store_course = StoreCourse.objects.get(course=course, store=store)
                        except Catalog.StoreCourse:
                            inserted_item.errors[store_slug + '__' + course] = ['store_course did not find']
                            inserted_item.save()
                        else:
                            try:
                                course_catalog = CourseCatalog.objects.get(catalog=catalog, store_course=store_course)
                            except CourseCatalog.DoesNotExist:
                                course_catalog_serializer = CourseCatalogSerializer(data={'catalog': catalog.id, 'store_course': store_course.id})
                                if course_catalog_serializer.is_valid():
                                    course_catalog_serializer.save()
                                    inserted_item.errors[store_slug + '_course_catalog'] = ['course_catalog created successfully']
                                    inserted_item.save()
                                else:
                                    inserted_item.errors[store_slug + '_course_catalog'] = course_catalog_serializer.errors
                                    inserted_item.save()
                            else:
                                pass
                else:
                    inserted_item.errors[store_slug] = catalog_serializer.errors
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                inserted_item.save()

        return True
