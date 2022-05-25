from rest_framework import serializers
from rest_framework_mongoengine.serializers import DocumentSerializer, EmbeddedDocumentSerializer

from shared_models.models import Course, Section, Product, Catalog, CourseCatalog, QuestionBank
from models.course.section import Section as SectionModel
from models.courseprovider.course_provider import CourseProvider as CourseProviderModel
from models.course.course import Course as CourseModel
from models.courseprovider.instructor import Instructor as InstructorModel
from models.course.section_schedule import SectionSchedule as SectionScheduleModel
from models.publish.publish_job import PublishJob as PublishJobModel
from models.log.publish_log import PublishLog as PublishLogModel

from rest_framework_mongoengine.fields import ReferenceField
from django.utils.text import slugify


class ProductSerializer(serializers.ModelSerializer):

    class Meta:
        model = Product
        fields = ('id', 'store', 'external_id', 'product_type', 'title', 'content', 'image', 'limit_applicable',
            'total_quantity', 'quantity_sold', 'available_quantity', 'tax_code', 'tax_code', 'fee', 'minimum_fee',
            'currency_code')


class CourseSerializer(serializers.ModelSerializer):

    class Meta:
        model = Course
        fields = (
            'id',
            'course_provider',
            'title',
            'content_ready',
            'content_db_reference',
            'course_image_uri',
            'external_image_url'
        )

    def create(self, validated_data):
        validated_data['slug'] = slugify(validated_data['title'], allow_unicode=False)
        return Course.objects.create(**validated_data)


class CourseModelSerializer(DocumentSerializer):
    provider = ReferenceField(CourseProviderModel)

    class Meta:
        model = CourseModel
        exclude = ('slug',)

    def create(self, validated_data):
        validated_data['slug'] = slugify(validated_data['title'], allow_unicode=False)
        return CourseModel.objects.create(**validated_data)


class SectionSerializer(serializers.ModelSerializer):

    class Meta:
        model = Section
        fields = ('id', 'course', 'name', 'fee', 'seat_capacity', 'available_seat',
                  'execution_mode', 'registration_deadline', 'content_db_reference', 'is_active')


class CheckSectionModelValidationSerializer(EmbeddedDocumentSerializer):

    class Meta:
        model = SectionModel


class InstructorModelSerializer(DocumentSerializer):
    provider = ReferenceField(CourseProviderModel)

    class Meta:
        model = InstructorModel
        fields = ('id', 'provider', 'name', 'external_id', 'profile_urls', 'image', 'short_bio', 'detail_bio')


class SectionScheduleModelSerializer(EmbeddedDocumentSerializer):

    class Meta:
        model = SectionScheduleModel


class PublishJobModelSerializer(DocumentSerializer):

    class Meta:
        model = PublishJobModel


class PublishLogModelSerializer(DocumentSerializer):
    publish_job_id = ReferenceField(PublishJobModel)

    class Meta:
        model = PublishLogModel


class CatalogSerializer(serializers.ModelSerializer):

    class Meta:
        model = Catalog
        fields = '__all__'


class CourseCatalogSerializer(serializers.ModelSerializer):

    class Meta:
        model = CourseCatalog
        fields = '__all__'


class QuestionBankSerializer(serializers.ModelSerializer):
    class Meta:
        model = QuestionBank
        fields = ('id', 'provider_ref', 'provider_type', 'external_id', 'title', 'question_type', 'configuration')
