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
    PublishLogModelSerializer, ProductSerializer, CourseCatalogSerializer, CatalogSerializer

from config.mongo_client import connect_mongodb, disconnect_mongodb

from shared_models.models import Course, CourseProvider, Section, CourseSharingContract, StoreCourse, Product,\
    StoreCourseSection, Store, RelatedProduct, CourseCatalog, Catalog, QuestionBank
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.courseprovider.provider_site import CourseProviderSite as CourseProviderSiteModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.course import Course as CourseModel
from models.course.section import Section as SectionModel
from models.log.publish_log import PublishLog as PublishLogModel
from datetime import datetime
import decimal
from django.utils import timezone
from django.utils.text import slugify

from django_scopes import scopes_disabled
from models.publish.publish_job import PublishJob as PublishJobModel
from mongoengine import NotUniqueError
from django.db import transaction
from django_initializer import initialize_django

initialize_django()

class UpdateData():
    def update_courses(self, doc, item, course_provider, course_provider_model, contracts=[]):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'course_update', 'time': timezone.now(),
            'message':'task is still in queue', 'status': 'pending', 'external_id': item['match'].get('course', '')
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
            course_model = CourseModel.objects.get(external_id=str(item['match']['course']), provider=course_provider_model)
        except CourseModel.DoesNotExist:
            inserted_item.errors = {'course': ['course_model does not found in database']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False
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
                return False

        else:
            inserted_item.errors = course_model_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        course_data = prepare_course_postgres(course_model, course_provider)

        with scopes_disabled():
            try:
                course = Course.objects.get(content_db_reference=course_model.id, course_provider=course_provider)
            except Course.DoesNotExist:
                inserted_item.errors = {'course': ['course does not found']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
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
                return False

            # create StoreCourse
            for contract in contracts:
                store_course, created = StoreCourse.objects.get_or_create(
                    course=course,
                    store=contract.store,
                    defaults={'enrollment_ready': True, 'is_featured': False, 'is_published': False}
                )
                if not created:
                    store_course_sections = StoreCourseSection.objects.filter(store_course=store_course.id)
                    for object in store_course_sections:
                        try:
                            product = object.product
                        except KeyError:
                            pass
                        else:
                            product.title = course.title
                            product.save()

        return True


    def update_sections(self, doc, data, course_provider, course_provider_model, contracts=[]):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'section_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['match'].get('section', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(external_id=str(data['match']['course']), provider=course_provider_model)
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['corresponding course model does not found']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        section_model_data = None
        section_model_code = None
        fee = 0.0

        for section in course_model.sections:
            if section.external_id == str(data['match']['section']):
                section_model_data = section
                section_model_code = section_model_data['code']
                break


        # if key "fee" is not present in payload, then manually set fee from the mongo object
        if section_model_data:
            try:
                fee = section_model_data['course_fee']['amount']
            except KeyError:
                pass

        data['data']['course_fee'] = {'amount': data['data'].get('fee', fee), 'currency': 'USD'}

        with scopes_disabled():
            try:
                course = Course.objects.get(content_db_reference=str(course_model.id), course_provider=course_provider)
            except Course.DoesNotExist:
                # without that we can not proceed comfortably
                inserted_item.errors = {'parent': ['corresponding course does not found']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

        # now update the sections in mongo
        section_model_serializer = CheckSectionModelValidationSerializer(section_model_data, data=data['data'], partial=True)

        if section_model_serializer.is_valid():
            section_model_serializer.save()
        else:
            inserted_item.errors = section_model_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        if course_model.sections:
            for section_idx, sec_data in enumerate(course_model.sections):
                if sec_data['external_id'] == str(data['match']['section']):
                    new_section_data = sec_data.to_mongo().to_dict()
                    new_section_data.update(section_model_serializer.data)
                    course_model.sections[section_idx] = SectionModel(**new_section_data)
                    break
            else:
                inserted_item.errors = {'section': ['section does not found in course model']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
        else:
            inserted_item.errors = {'section': ['no section found with the corresponding course']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False


        section_data = prepare_section_postgres(section_model_serializer.data, data['data'].get('fee', fee),  course, course_model)
        with scopes_disabled():
            try:
                section = course.sections.get(name=section_model_code)
            except Section.DoesNotExist:
                inserted_item.errors = {'section': ['section does not found in course']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False
            else:
                serializer = SectionSerializer(section, data=section_data, partial=True)

            if serializer.is_valid():
                section = serializer.save()
                course_model.save()
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

                    # delete previous related products for this product if available
                    linked_related_products = RelatedProduct.objects.filter(product=product)
                    linked_related_products.delete()

                    # create related products for this product with given related_products
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
        return True


    def update_schedules(self, doc, data, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'schedule_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['match'].get('schedule', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(
                external_id=str(data['match']['course']),
                sections__external_id=str(data['match']['section']),
                provider=course_provider_model
            )
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

        # check if the provided data is valid. if not, abort.
        schedule_serializer = SectionScheduleModelSerializer(data=data['data'])
        if not schedule_serializer.is_valid():
            inserted_item.errors = schedule_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        for section_idx, section in enumerate(course_model.sections):
            if section['external_id'] == str(data['match']['section']):
                serializer = CheckSectionModelValidationSerializer(section)
                if serializer.data['schedules']:
                    for schedule_idx, schedule in enumerate(serializer.data['schedules']):
                        if schedule['external_id'] == str(data['match']['schedule']):
                            serializer.data['schedules'][schedule_idx].update(schedule_serializer.data)
                            break
                    else:
                        inserted_item.errors = {'schedule': ['schedule not found']}
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        return False
                else:
                    inserted_item.errors = {'schedule': ['no schedule found with the section']}
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    return False

                course_model.sections[section_idx] = SectionModel(**serializer.data)
                course_model.save()
                inserted_item.message = 'task processed successfully'
                inserted_item.status = 'completed'
                inserted_item.save()
                break
        return True


    def update_instructors(self, doc, data, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'instructor_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['match'].get('instructor', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            course_model = CourseModel.objects.get(
                external_id=str(data['match']['course']), sections__external_id=str(data['match']['section']),
                provider=course_provider_model
            )
        except CourseModel.DoesNotExist:
            # without that we can not proceed comfortably
            inserted_item.errors = {'parent': ['corresponding course does not found']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        data['data']['provider'] = course_provider_model.id
        try:
            instructor_model = InstructorModel.objects.get(
                external_id=str(data['match']['instructor']), provider=course_provider_model
            )
        except InstructorModel.DoesNotExist:
            inserted_item.errors = {'instructor': ['instructor does not found']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False
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

        return True


    def update_products(self, doc, item, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'product_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': data['match'].get('product', '')
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
                    product = Product.objects.get(
                        external_id= str(item['match']['product']), store=store,
                        product_type=item['data']['product_type']
                    )
                except Product.DoesNotExist:
                    inserted_item.errors = {'product': ['corresponding product does not found']}
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    return False
                else:
                    product_serializer = ProductSerializer(product, data=product_data, partial=True)

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


    def update_subjects(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'subject_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['data'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        data = item['data']
        catalog_slug = slugify(item['match'].get('title', ''))
        store_slug = item['match'].get('store', '')

        if data.get('title', False):
            data['slug'] = slugify(data['title'])


        courses = []
        #getting courses from given course external_id
        for tagging_course in item['related_records']:
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

        # find out the catalog and update accordingly
        try:
            store = Store.objects.get(url_slug=store_slug)
        except Store.DoesNotExist:
            inserted_item.errors[store_slug] = ['corresponding store does not found in database']
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False
        else:
            with scopes_disabled():
                try:
                    catalog = Catalog.objects.get(store=store.id, slug=catalog_slug)
                except Catalog.DoesNotExist:
                    inserted_item.errors[store_slug] = ['corresponding catalog does not found in database']
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    return False
                else:
                    catalog_serializer = CatalogSerializer(catalog, data=data, partial=True)

                    if catalog_serializer.is_valid():
                        catalog = catalog_serializer.save()
                        inserted_item.message = 'subject created successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()

                        # first delete all course catalog for this catalog
                        course_catalog = CourseCatalog.objects.filter(catalog=catalog)
                        course_catalog.delete()

                        # then create course catalogs for all the given courses with this catalog
                        for course in courses:
                            try:
                                store_course = StoreCourse.objects.get(course=course, store=store)
                            except StoreCourse.DoesNotExist:
                                inserted_item.errors['course'] = ['course with external_id ' + course_models[
                                    idx].external_id + ' is not published in store ' + store_slug]
                                inserted_item.save()

                            else:
                                course_catalog_serializer = CourseCatalogSerializer(
                                    data={'catalog': catalog.id, 'store_course': store_course.id}
                                )
                                if course_catalog_serializer.is_valid():
                                    course_catalog_serializer.save()
                                    inserted_item.message = inserted_item.message + '' + os.linesep +\
                                                            ' catalog successfully tagged with course with external_id'\
                                                            + course_models[idx].external_id
                                    inserted_item.save()
                                else:
                                    inserted_item.errors[store_slug + '_course_catalog'] = course_catalog_serializer.errors
                                    inserted_item.save()
                    else:
                        inserted_item.errors[store_slug] = catalog_serializer.errors
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                    inserted_item.save()

        return True


    def update_questions(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'question_update', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': item['match'].get('external_id', '')
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        data = item['data']

        try:
            question = QuestionBank.objects.get(external_id=item['match'].get('external_id', ''), provider_ref=course_provider.id)
        except QuestionBank.DoesNotExist:
            inserted_item.errors = {'question': ['question does not found']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
            return False

        else:
            question_bank_serializer = QuestionBankSerializer(question, data=data, partial=True)

        if question_bank_serializer.is_valid():
            question_bank_serializer.save()
            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
        else:
            inserted_item.errors = product_serializer.errors
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
        inserted_item.save()

        return True
