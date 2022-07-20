from django.utils import timezone
from django_scopes import scopes_disabled
from shared_models.models import QuestionBank, Course, ProfileQuestion, RegistrationQuestion, Store, PaymentQuestion

from models.course.course import Course as CourseModel

from .serializers import ProfileQuestionSerializer, RegistrationQuestionSerializer, PaymentQuestionSerializer,\
    PublishLogModelSerializer


class UntagData():
    def untag_question(self, doc, course_provider, course_provider_model, data):
        # insert every item in mongo to get status individually
        external_id = data['match'].get('question', None)
        mongo_data = {
            'data': data, 'publish_job_id': doc['id'], 'type': 'question_untag', 'time': timezone.now(),
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
            inserted_item.errors = {'question': ['question with the external_id not found for this provider']}
            inserted_item.status = 'failed'
            inserted_item.message = 'error occurred'
            inserted_item.save()
        else:
            tag_map = data.get('tag_map', None)
            question_type = tag_map.get('question_type', None)
            tag_with = data.get('tag_with', None)

            if tag_map and question_type:
                # untag profile question
                if question_type == 'profile':
                    try:
                        p_question = ProfileQuestion.objects.get(question_bank=question.id, provider_ref=course_provider.id, respondent_type= tag_map.get('question_for', None))
                    except ProfileQuestion.DoesNotExist:
                        inserted_item.errors = {'profile_question': ['profile_question not found']}
                        inserted_item.type = 'profile_question_untag'
                        inserted_item.status = 'failed'
                        inserted_item.message = 'error occurred'
                        inserted_item.save()
                        return False
                    else:
                        p_question.delete()
                        inserted_item.type = 'profile_question_untag'
                        inserted_item.message = 'task processed successfully'
                        inserted_item.status = 'completed'
                        inserted_item.save()
                        return True

                # untag registration question
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
                                inserted_item.type = 'registration_question_untag'
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
                                        inserted_item.type = 'registration_question_untag'
                                        inserted_item.status = 'failed'
                                        inserted_item.message = 'error occurred'
                                        inserted_item.save()
                                        return False

                            # then untag registration question
                            try:
                                r_question = RegistrationQuestion.objects.get(question_bank=question.id, entity_id=course.id)
                            except RegistrationQuestion.DoesNotExist:
                                inserted_item.errors = {'registration_question': ['registration_question not found']}
                                inserted_item.type = 'registration_question_untag'
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                                return False
                            else:
                                r_question.delete()
                                inserted_item.type = 'registration_question_untag'
                                inserted_item.message = 'task processed successfully'
                                inserted_item.status = 'completed'
                                inserted_item.save()
                                return True

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

                            # then untag payment question
                            try:
                                pay_question = PaymentQuestion.objects.get(question_bank=question.id, store=store.id)
                            except PaymentQuestion.DoesNotExist:
                                inserted_item.errors = {'paymentQuestion_question': ['paymentQuestion_question not found']}
                                inserted_item.type = 'paymentQuestion_question_untag'
                                inserted_item.status = 'failed'
                                inserted_item.message = 'error occurred'
                                inserted_item.save()
                                return False
                            else:
                                r_question.delete()
                                inserted_item.type = 'paymentQuestion_question_untag'
                                inserted_item.message = 'task processed successfully'
                                inserted_item.status = 'completed'
                                inserted_item.save()
                                return True

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
