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

from .serializers import CourseSerializer, SectionSerializer, CourseModelSerializer, QuestionBankSerializer,\
    CheckSectionModelValidationSerializer, InstructorModelSerializer, SectionScheduleModelSerializer,\
    PublishLogModelSerializer, ProductSerializer, CatalogSerializer, CourseCatalogSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product,\
    StoreCourseSection, Store, RelatedProduct, Catalog, CourseCatalog, QuestionBank
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from models.log.publish_log import PublishLog as PublishLogModel
from datetime import datetime
import decimal
import os
from django.utils import timezone
import mongoengine

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

                try:
                    course_model = CourseModel.objects.get(
                        external_id=str(data.get('external_id', '')),
                        provider=course_provider_model
                    )
                except CourseModel.DoesNotExist:
                    course_model_serializer = CourseModelSerializer(data=data)
                else:
                    course_model_serializer = CourseModelSerializer(course_model, data=data, partial=True)

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
                    try:
                        course = Course.objects.get(
                            content_db_reference=course_model.id,
                            course_provider=course_provider
                        )
                    except Course.DoesNotExist:
                        course_serializer = CourseSerializer(data=course_data)
                    else:
                        course_serializer = CourseSerializer(course, data=course_data, partial=True)

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
        external_id = str(data['data'].get('external_id', data['parent'].get('course', '')))
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'section_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': external_id
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        # Access data from MongoDB
        try:
            course_model = CourseModel.objects.get(external_id=data['parent']['course'], provider=course_provider_model)
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['invalid parent in section']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        section_model_data = None
        section_model_code = None
        section_model_external_id = None
        fee = 0.0

        for section in course_model.sections:
            if section.external_id == str(data['data'].get("external_id", '')):
                section_model_data = section
                section_model_code = section['code']
                section_model_external_id = section['external_id']
                break

        if section_model_data:
            try:
                fee = section_model_data['course_fee']['amount']
            except KeyError:
                pass

        data['data']['course_fee'] = {'amount': data['data'].get('fee', fee), 'currency': 'USD'}

        if section_model_data:
            section_model_serializer = CheckSectionModelValidationSerializer(
                section_model_data, data=data['data'], partial=True
            )
        else:
            section_model_serializer = CheckSectionModelValidationSerializer(data=data['data'])
        if section_model_serializer.is_valid():
            section_model_serializer.save()
        else:
            inserted_item.errors = section_model_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        # Access data and upsert in postgres
        with scopes_disabled():
            try:
                course = Course.objects.get(content_db_reference=str(course_model.id), course_provider=course_provider)
            except Course.DoesNotExist:
                # without that we can not proceed comfortably
                inserted_item.errors = {'parent': ['corresponding course not found']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

            section_data = prepare_section_postgres(
                section_model_serializer.data, data['data'].get('fee', fee), course, course_model
            )
            try:
                section = course.sections.get(name=section_model_code)
            except Section.DoesNotExist:
                serializer = SectionSerializer(data=section_data)
            else:
                serializer = SectionSerializer(section, data=section_data, partial=True)

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

        # section upsert completed in postgres
        # now upsert the section in mongo
        if course_model.sections:
            for section_idx, sec_data in enumerate(course_model.sections):
                if sec_data['external_id'] == section_model_external_id:
                    new_section_data = sec_data.to_mongo().to_dict()
                    new_section_data.update(section_model_serializer.data)
                    course_model.sections[section_idx] = SectionModel(**new_section_data)
                    break
            else:
                CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=section_model_serializer.data)
        else:
            CourseModel.objects(id=course_model.id).update_one(add_to_set__sections=section_model_serializer.data)

        course_model.save()

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

                    # delete previous related products for this product if available
                    linked_related_products = RelatedProduct.objects.filter(product=product)
                    linked_related_products.delete()

                    # create related products for this product with given related_products
                    for related_product in related_products:
                        if related_product['type'] == 'product':
                            # create new task to get status for all related records
                            external_id = str(related_product.get('external_id', 'None'))
                            mongo_data = {
                                'data': data, 'publish_job_id': doc['id'], 'type': 'related_product_create',
                                'time': timezone.now(),
                                'message': 'task is still in queue', 'status': 'pending', 'external_id': external_id
                            }

                            log_serializer = PublishLogModelSerializer(data=mongo_data)
                            if log_serializer.is_valid():
                                inserted_item = log_serializer.save()
                            else:
                                print(log_serializer.errors)

                            # find the child products first
                            try:
                                child_product = Product.objects.get(
                                    external_id=related_product['external_id'],
                                    product_type=related_product['external_type']
                                )
                            except Product.DoesNotExist:
                                inserted_item.errors = {'product': ['no product available with the external_id and external_type']}
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()

                            except Product.MultipleObjectsReturned:
                                inserted_item.errors = {'product': ['multiple products with the same external_id and external_type']}
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                            except Exception as exc:
                                inserted_item.errors = {'error': ['unknown error occurred when searching related product']}
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                            else:
                                try:
                                    related_product = RelatedProduct.objects.create(
                                        product=product,
                                        related_product=child_product,
                                        related_product_type=related_product['relation_type']
                                    )
                                except Exception as exc:
                                    inserted_item.errors = {'error': ['unknown error occurred when creating related product']}
                                    inserted_item.status = 'failed'
                                    inserted_item.message = 'error occurred'
                                    inserted_item.save()
                                else:
                                    inserted_item.message = 'task processed successfully'
                                    inserted_item.status = 'completed'
                                    inserted_item.save()


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
            inserted_item.errors = {'course': ['invalid parent course in schedule']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        except CourseModel.MultipleObjectsReturned:
            # without that we can not proceed comfortably
            inserted_item.errors = {'external_id': ['many courses with the same external_id']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        # try:
        #     data['data']['start_at'] = get_datetime_obj(data['data']['start_at'], inserted_item=inserted_item)
        #     if not data['data']['start_at']:
        #         inserted_item.errors = {'start_at': ['invalid date']}
        #         inserted_item.status = 'failed'
        #         inserted_item.message = 'error occurred'
        #         inserted_item.save()
        #         return False
        # except KeyError:
        #     data['data']['start_at'] = None
        #
        # try:
        #     data['data']['end_at'] = get_datetime_obj(data['data']['end_at'], inserted_item=inserted_item)
        #     if not data['data']['end_at']:
        #         inserted_item.errors = {'end_at': ['invalid date']}
        #         inserted_item.status = 'failed'
        #         inserted_item.message =
        #         'error occurred'
        #         inserted_item.save()
        #         return False
        # except KeyError:
        #     data['data']['end_at'] = None

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
                if serializer.data['schedules']:
                    for schedule_idx, schedule in enumerate(serializer.data['schedules']):
                        if schedule['external_id'] == str(data['data']['external_id']):
                            serializer.data['schedules'][schedule_idx].update(schedule_serializer.data)
                            break
                    else:
                        serializer.data['schedules'].append(schedule_serializer.data)
                else:
                    serializer.data['schedules'].append(schedule_serializer.data)

                course_model.sections[section_idx] = SectionModel(**serializer.data)
                course_model.save()
                inserted_item.message = 'task processed successfully'
                inserted_item.status = 'completed'
                inserted_item.save()
                break
        return True


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

        # upsert data
        try:
            instructor_model = InstructorModel.objects.get(
                external_id=str(data['data']['external_id']), provider=course_provider_model
            )
        except InstructorModel.DoesNotExist:
            instructor_model_serializer = InstructorModelSerializer(data=data['data'])
        else:
            instructor_model_serializer = InstructorModelSerializer(instructor_model, data=data['data'], partial=True)

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
                for instructor in section['instructors']:
                    if instructor == instructor_model:
                        break
                else:
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


    def create_subjects(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'subject_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        data = item.get('data', [])
        data['slug'] = slugify(data['title'])
        data['from_importer'] = True
        # description is required at C4I, if not provided by partner, we will put tiltle value here
        data['description'] = data.get('description', data['title'])

        # getting courses from given course external_id
        courses = []
        course_models = []
        for tagging_course in item.get('related_records', []):
            if tagging_course.get('type', '') == 'course':
                try:
                    course_model = CourseModel.objects.get(
                        external_id=tagging_course.get('external_id', ''),
                        provider=course_provider_model
                    )
                except CourseModel.DoesNotExist:
                    inserted_item.errors[tagging_course] = ['course with external_id ' + tagging_course + ' not found']
                    inserted_item.save()
                else:
                    with scopes_disabled():
                        try:
                            course = Course.objects.get(
                                content_db_reference=str(course_model.id),
                                course_provider=course_provider
                            )
                        except Course.DoesNotExist:
                            inserted_item.errors[tagging_course] = [
                                'course with external_id ' + tagging_course + ' not found']
                            inserted_item.save()
                        else:
                            courses.append(course)
                            course_models.append(course_model)

        # getting store from given store_slug list
        # create catalog for that store
        # tag catalog with store course

        for store_slug in item.get('publishing_stores', []):
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

            # upsert catalog for that store
            with scopes_disabled():
                try:
                    catalog = Catalog.objects.get(store=data['store'], slug=data['slug'])
                except Catalog.DoesNotExist:
                    catalog_serializer = CatalogSerializer(data=data)
                else:
                    catalog_serializer = CatalogSerializer(catalog, data=data, partial=True)

                if catalog_serializer.is_valid():
                    catalog = catalog_serializer.save()
                    inserted_item.message = 'subject created successfully'
                    inserted_item.status = 'completed'
                    inserted_item.save()

                    # untag catalog with store course
                    CourseCatalog.objects.filter(catalog=catalog).delete()

                    # tag catalog with store course
                    for idx, course in enumerate(courses):
                        try:
                            store_course = StoreCourse.objects.get(course=course, store=store)
                        except StoreCourse.DoesNotExist:
                            inserted_item.errors['course'] = ['course with external_id '+ course_models[
                                idx].external_id +' is not published in store '+ store_slug]
                            inserted_item.save()
                        else:
                            try:
                                course_catalog = CourseCatalog.objects.get(catalog=catalog, store_course=store_course)
                            except CourseCatalog.DoesNotExist:
                                course_catalog_serializer = CourseCatalogSerializer(data={
                                    'catalog': catalog.id, 'store_course': store_course.id
                                })
                                if course_catalog_serializer.is_valid():
                                    course_catalog_serializer.save()
                                    inserted_item.message = inserted_item.message + '' + os.linesep +\
                                                            ' catalog successfully tagged with course with external_id '\
                                                            + course_models[idx].external_id
                                    inserted_item.save()
                                else:
                                    inserted_item.errors[store_slug + '_course_catalog'] = course_catalog_serializer.errors
                                    inserted_item.save()
                            else:
                                inserted_item.message = inserted_item.message + '' + os.linesep +\
                                                        ' catalog already tagged with course with external_id' +\
                                                        course_models[idx].external_id
                                inserted_item.save()
                else:
                    inserted_item.errors[store_slug] = catalog_serializer.errors
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                inserted_item.save()

        return True


    def create_questions(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'question_create', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        data = item['data']
        data['provider_type'] = 'course_provider'
        data['provider_ref'] = course_provider.id
        data['question_type'] = data['input'].get('type', None)
        data['configuration'] = data['input'].get('config', {})
        autocomplete = data['configuration'].get('autocomplete', False)

        try:
            question = QuestionBank.objects.get(external_id=data['external_id'], provider_ref=data['provider_ref'])
        except QuestionBank.DoesNotExist:
            question_bank_serializer = QuestionBankSerializer(data=data)
        else:
            question_bank_serializer = QuestionBankSerializer(question, data=data, partial=True)

        collection = 'question_bank_options'
        if question_bank_serializer.is_valid():
            qbank = question_bank_serializer.save()
            if qbank.question_type == 'select' and autocomplete:
                data = {
                    'question_bank': str(qbank.id),
                    'options': qbank.configuration.get('autocomplete_options', []),
                    'datetime': datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                }
                query = {'question_bank': str(qbank.id)}

                db = mongoengine.get_db()
                coll = db[collection]
                doc = coll.find_one(query)

                if doc is None:
                    coll.insert_one(data)
                else:
                    coll.update_one(query, {'$set': data}, upsert=True)

            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
        else:
            inserted_item.errors = question_bank_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
        inserted_item.save()

        return True
