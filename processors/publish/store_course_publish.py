from decouple import config
from .serializers import PublishLogModelSerializer

from shared_models.models import Course, Section, CourseSharingContract, StoreCourse, Product, StoreCourseSection,\
    Store, SharedCourse
from models.course.course import Course as CourseModel
from models.log.publish_log import PublishLog as PublishLogModel

from django.utils import timezone
from django.shortcuts import get_object_or_404
import requests

from django_scopes import scopes_disabled
from django.db import transaction
from django_initializer import initialize_django

initialize_django()

class StoreCoursePublish():
    def course_publish_in_stores(self, doc, data, course_provider, course_provider_model):
        for store_slug in data['publishing_stores']:
            # insert every item in mongo to get status individually
            mongo_data = {'data': data, 'publish_job_id': doc['id'], 'type': 'course_publishing_'+store_slug, 'time': timezone.now(),
                          'message': 'task is still in queue', 'status': 'pending', 'external_id': str(data['data']['external_id'])}

            log_serializer = PublishLogModelSerializer(data=mongo_data)
            if log_serializer.is_valid():
                inserted_item = log_serializer.save()
            else:
                print(log_serializer.errors)

            store = get_object_or_404(Store, url_slug=store_slug)

            try:
                course_obj = CourseModel.objects.get(
                    external_id=data['data']['external_id'],
                    provider=course_provider_model
                )
            except CourseModel.DoesNotExist:
                # without that we can not proceed comfortably
                inserted_item.errors = {'parent': ['course model does not found']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

            with scopes_disabled():
                try:
                    course = Course.objects.get(content_db_reference=course_obj.id, course_provider=course_provider.id)
                except CourseModel.DoesNotExist:
                    # without that we can not proceed comfortably
                    inserted_item.errors = {'course': ['course does not found']}
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    return False

                is_published = data['data']['is_published']
                try:
                    store_course = StoreCourse.objects.get(store=store, course=course)
                except StoreCourse.DoesNotExist:
                    # create a new StoreCourse e.g. publish

                    # checking if course sharing contract exists
                    try:
                        contract = CourseSharingContract.objects.get(
                            course_provider=course.course_provider, store=store
                        )
                    except CourseSharingContract.DoesNotExist:
                        inserted_item.errors = {'contract': ['Course Provider Sharing Contract does not exist']}
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        return False

                    # if course does not have any sections, it cannot be published.

                    if not course.sections.all().exists():
                        inserted_item.errors = {'section': ['This course does not have any sections']}
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        return False

                    with transaction.atomic():
                        # StoreCourse entry
                        store_course = StoreCourse.objects.create(
                            course=course,
                            store=store,
                            is_published=is_published,
                            enrollment_ready=True,
                            is_featured=False,
                            display_order=1,
                        )

                        try:
                            SharedCourse.objects.get(course=course, sharing_contract=contract)
                        except SharedCourse.DoesNotExist:
                            SharedCourse.objects.create(
                                course=course, sharing_contract=contract, is_active=True
                            )

                        # create StoreCourseSection and product
                        for section in course.sections.all():
                            try:
                                store_course_section = StoreCourseSection.objects.get(
                                    store_course=store_course,
                                    section=section
                                )
                            except StoreCourseSection.DoesNotExist:
                                product = Product.objects.create(
                                    store=store,
                                    external_id=course_obj.external_id,
                                    product_type=Product.PRODUCT_TYPE_SECTION,
                                    title=course.title,
                                    content={},
                                    limit_applicable=False,
                                    total_quantity=section.seat_capacity,
                                    quantity_sold=0,
                                    available_quantity=section.seat_capacity,
                                    tax_code=config('AVATAX_TAX_CODE', 'ST080031'),
                                    fee=section.fee,
                                    minimum_fee=section.fee,
                                    currency_code='usd'
                                )

                                StoreCourseSection.objects.create(
                                    store_course=store_course,
                                    section=section,
                                    is_published=store_course.is_published,
                                    product=product
                                )
                            else:
                                store_course_section.is_published = store_course.is_published
                                store_course_section.active_status = True
                                store_course_section.save()

                else:
                    # StoreCouse has an entry with this course and store already. Therefore, it was already 'published'.
                    # we will now just update that entry's attributes

                    with transaction.atomic():
                        store_course.is_published = is_published
                        store_course.enrollment_ready = True
                        store_course.is_featured = False
                        store_course.display_order = 1
                        store_course.active_status = True
                        store_course.save()

                        # we will update the StoreCouseSection as well
                        for section in course.sections.all():
                            try:
                                store_course_section = StoreCourseSection.objects.get(
                                    store_course=store_course,
                                    section=section.id
                                )
                            except StoreCourseSection.DoesNotExist:
                                pass
                                # what can you do if a section with the given pk doesn't exist?
                            else:
                                product = store_course_section.product
                                product.fee = section.fee
                                product.minimum_fee = section.fee
                                product.active_status = True
                                product.save()

                                store_course_section.active_status = True
                                store_course_section.save()

            # update the course object in mongodb too
            course_obj._is_published = is_published
            course_obj.save()
            course.content_ready = True
            course.save()
            inserted_item.message = 'task processed successfully'
            inserted_item.status = 'completed'
            inserted_item.save()

            # update the record into elasticsearech too

            if is_published:
                self.es_course_publish(store_course)
            else:
                self.es_course_unpublish(store_course)

        return True



    def es_course_publish(self, store_course):
        '''
        checks if the course already exists or not. if it does, the id of the store in which the course is being published is
        appended to course['stores'] list. else a new course is created
        '''
        baseURL = config('ES_BASE_URL')
        method = 'GET'
        db_ref = store_course.course.content_db_reference
        url = f'{baseURL}/course/_doc/{db_ref}?routing={db_ref}'
        resp = requests.get(url)
        course = resp.json()

        if course['found']:
            try:
                stores = course['_source']['stores']
            except KeyError:
                stores = [store_course.store.url_slug]
            else:
                if store_course.store.url_slug not in stores:
                    stores.append(store_course.store.url_slug)

            url = url.replace('_doc', '_update')
            payload = {
                'doc': {
                    "stores": stores
                }
            }

            method = 'POST'

        else:
            mongo_course = CourseModel.objects.get(id=store_course.course.content_db_reference)
            payload = {
                'title': store_course.course.title,
                'sortable_title': store_course.course.title,
                'description': mongo_course.description,
                'skills': mongo_course.skills,
                'careers': mongo_course.careers,
                'online': True,
                'self_paced': False,
                'provider_id': str(store_course.course.course_provider.content_db_reference),
                'provider_name': store_course.course.course_provider.name,
                'geo_location': [],
                'city_state_zip': [],
                'stores': [store_course.store.url_slug, ]
            }

            method = 'PUT'

        resp = requests.request(method, url, json=payload)
        return resp


    def es_course_unpublish(self, store_course):
        '''
        checks the stores key in the course object and removes the store id of the store from which the course is being unpublished.
        if the store id is the sole item, then the whole key is removed
        '''
        baseURL = config('ES_BASE_URL')
        method = 'GET'
        db_ref = store_course.course.content_db_reference
        url = f'{baseURL}/course/_doc/{db_ref}?routing={db_ref}'
        resp = requests.get(url)
        course = resp.json()

        if course['found']:
            try:
                stores = course['_source']['stores']
            except KeyError:
                pass
            else:
                if store_course.store.url_slug in stores:
                    stores.remove(store_course.store.url_slug)

                    if len(stores) == 0:
                        method = 'DELETE'
                        resp = requests.request(method, url)
                    else:
                        url = url.replace('_doc', '_update')
                        payload = {
                            'doc': {
                                "stores": stores
                            }
                        }

                        method = 'POST'
                        resp = requests.request(method, url, json=payload)

        return resp
