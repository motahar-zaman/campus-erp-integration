import csv
import urllib.request
from models.course.course import Course as CourseModel
from bson import ObjectId


def import_courses(import_task):
    filename = import_task.filename.name
    remote_url = 'https://static.dev.campus.com/uploads/'
    file_url = f'{remote_url}{filename}'

    response = urllib.request.urlopen(file_url)
    lines = [line.decode('utf-8') for line in response.readlines()]
    cr = csv.DictReader(lines)
    items = list(cr)

    for data in items:
        data = {k.strip(): v.strip() for (k, v) in data.items()}
        data['image'] = {'original': data['image']}
        data['default_image'] = {'original': data['default_image']}
        data['from_importer'] = True
        data['keywords'] = []
        data['provider'] = ObjectId(import_task.course_provider.content_db_reference)
        try:
            course_model = CourseModel.objects.get(slug=data['slug'])
        except CourseModel.DoesNotExist:
            course_model = CourseModel.objects.create(**data)
        else:
            course_model.update(**data)
