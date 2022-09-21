from mongoengine import get_db, connect, disconnect
from bson import ObjectId
from decouple import config
from django.utils import timezone
from shared_models.models import Course, StoreCourse, StoreCourseSection, Product
from django_scopes import scopes_disabled
import requests
from ..publish.helpers import es_course_unpublish
from django.db import transaction


def deactivate_course_sharing_contact(payload):
    course_provider = payload['course_provider']
    is_active = payload['is_active']
    store = payload['store']

    if not is_active:
        with scopes_disabled():
            courses = Course.objects.filter(course_provider=course_provider['id'])

            for course in courses:
                sections = course.sections.all()
                store_courses = StoreCourse.objects.filter(course=course.id, store=store['id'])
                store_course_sections = StoreCourseSection.objects.filter(store_course__in=store_courses, section__in=sections)

                with transaction.atomic():
                    store_course_sections.update(active_status=False)

                    Product.objects.filter(
                        id__in=[scs.product.id for scs in store_course_sections]
                    ).update(active_status=False)

                    store_courses.update(active_status=False, enrollment_ready=False)

                for store_course in store_courses:
                    es_course_unpublish(store_course)
    return True
