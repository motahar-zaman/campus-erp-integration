from django.utils import timezone
from django_scopes import scopes_disabled
from shared_models.models import QuestionBank, Course, ProfileQuestion, RegistrationQuestion, Store, PaymentQuestion

from models.course.course import Course as CourseModel

from .serializers import ProfileQuestionSerializer, RegistrationQuestionSerializer, PaymentQuestionSerializer,\
    PublishLogModelSerializer


class TagData():
    def tag_question(self, doc, item, course_provider, course_provider_model):
        # insert every item in mongo to get status individually
        external_id = item['match'].get('question', None)
        mongo_data = {
            'data': item, 'publish_job_id': doc['id'], 'type': 'question_tag', 'time': timezone.now(),
            'message': 'task is still in queue', 'status': 'pending', 'external_id': external_id
        }

        log_serializer = PublishLogModelSerializer(data=mongo_data)
        if log_serializer.is_valid():
            inserted_item = log_serializer.save()
        else:
            print(log_serializer.errors)

        try:
            question = QuestionBank.objects.get(external_id=str(external_id), provider_ref=course_provider.id)
        except QuestionBank.DoesNotExist:
            inserted_item.errors =  {'question': ['question with the external_is not found for this provider']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
        else:
            tag_map = item.get('tag_map', None)
            question_type = tag_map.get('question_type', None)
            tag_with = item.get('tag_with', None)

            if tag_map and question_type:
                # tag question with profile
                if question_type == 'profile':
                    data={
                        'question_bank': question.id,
                        'provider_type': 'course_provider',
                        'provider_ref': course_provider.id,
                        'display_order': 1,
                        'respondent_type': tag_map.get('question_for', None)
                    }

                    pq_serializer = ProfileQuestionSerializer(data=data)
                    if pq_serializer.is_valid():
                        pq_serializer.save()
                        inserted_item.type = 'profile_question_tag'
                        inserted_item.message = 'task processed successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()
                        return True
                    else:
                        inserted_item.errors = pq_serializer.errors
                        inserted_item.type = 'profile_question_tag'
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        return False

                # tag question with registration
                elif question_type == 'registration':
                    # find-out course
                    for tag in tag_with:
                        tag_with_type = tag.get('type', '')
                        if tag_with_type == 'course':
                            tag_with_external_id = tag.get('external_id', '')
                            try:
                                course_model = CourseModel.objects.get(
                                    external_id=str(tag_with_external_id),
                                    provider=course_provider_model
                                )
                            except CourseModel.DoesNotExist:
                                inserted_item.errors = {'course_model': ['course not found']}
                                inserted_item.type = 'registration_question_tag'
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                                return False
                            else:
                                with scopes_disabled():
                                    try:
                                        course = Course.objects.get(
                                            content_db_reference=course_model.id,
                                            course_provider=course_provider
                                        )
                                    except Course.DoesNotExist:
                                        inserted_item.errors = {'course': ['course not found']}
                                        inserted_item.type = 'registration_question_tag'
                                        inserted_item.status = 'failed'
                                        inserted_item.message = 'error occurred'
                                        inserted_item.save()
                                        return False

                            # then tag with registration
                            data = {
                                'entity_type': tag_with_type,
                                'entity_id': course.id,
                                'question_bank': question.id,
                                'display_order': 1
                            }

                            rq_serializer = RegistrationQuestionSerializer(data=data)
                            if rq_serializer.is_valid():
                                rq_serializer.save()
                                inserted_item.type = 'registration_question_tag'
                                inserted_item.message = 'task processed successfully'
                                inserted_item.status = 'completed'
                                inserted_item.save()
                                return True
                            else:
                                inserted_item.errors = rq_serializer.errors
                                inserted_item.type = 'registration_question_tag'
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                                return False

                # tag question with payment
                elif question_type == 'payment':
                    # find-out store
                    for tag in tag_with:
                        tag_with_type = tag.get('type', '')
                        if tag_with_type == 'store':
                            store_slug = tag.get('slug', '')
                            try:
                                store = Store.objects.get(url_slug=store_slug)
                            except Store.DoesNotExist:
                                inserted_item.errors = {'store': ['store not found']}
                                inserted_item.type = 'payment_question_tag'
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                                return False

                            data = {
                                'question_bank': question.id,
                                'store': store.id,
                                'display_order': 1
                            }

                            pay_serializer = PaymentQuestionSerializer(data=data)
                            if pay_serializer.is_valid():
                                pay_serializer.save()
                                inserted_item.message = 'task processed successfully'
                                inserted_item.type = 'payment_question_tag'
                                inserted_item.status = 'completed'
                                inserted_item.save()
                                return True
                            else:
                                inserted_item.errors = pay_serializer.errors
                                inserted_item.status = 'failed'
                                inserted_item.type = 'payment_question_tag'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                            return False

                else:
                    inserted_item.errors = {'tag_type': ['question_type is not valid']}
                    inserted_item.status = 'failed'
                    inserted_item.message = 'error occurred'
                    inserted_item.save()
                    return False

            else:
                inserted_item.errors = {'tag_map': ['required data not found in "tag_map" key in payload']}
                inserted_item.status = 'failed'
                inserted_item.message = 'error occurred'
                inserted_item.save()
                return False

        return True
