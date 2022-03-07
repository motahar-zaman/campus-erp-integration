from bson import ObjectId
from mongoengine import get_db
from decimal import Decimal

from datetime import datetime

def write_status(doc, status, collection='publish_job'):
    db = get_db()
    coll = db[collection]
    doc['status'] = status
    coll.update_one({'_id': doc['_id']}, {"$set": doc}, upsert=True)


def get_schedules(data):
    schedules = []
    for item in data:
        schedules.append({
            'section_type': item.get('section_type', 'LEC'),
            'external_version_id': item.get('external_version_id', ''),
            'name': item.get('name', ''),
            'description': item.get('description', ''),
            'start_at': get_datetime_obj(item.get('start_at')),
            'end_at': get_datetime_obj(item.get('end_at')),
            'building_name': item.get('building_name', ''),
            'building_code': item.get('building_code', ''),
            'room_name': item.get('room_name', ''),
        })
    return schedules


def get_instructors(data, course_provider_model):
    instructors = []
    for item in data:
        query = {'provider': course_provider_model.id, 'external_id': item.get('external_id', '')}
        data = {
            '_cls': 'Instructor',
            'provider': course_provider_model.id,
            'name': item.get('name', ''),
            'external_id': item.get('external_id', ''),
            'profile_urls': item.get('profile_urls', {}),
            'image': item.get('image', {}),
            'short_bio': item.get('short_bio', ''),
            'detail_bio': item.get('detail_bio', ''),
        }
        instructors.append(upsert_mongo_doc(collection='instructor', query=query, data=data))
    return instructors


def prepare_section_mongo(data, course_provider_model):
    section_data = []
    for item in data:
        try:
            num_seats = int(item.get('num_seats', 0))
        except ValueError:
            num_seats = 0

        try:
            course_fee = float(item.get('course_fee', 0.00))
        except ValueError:
            course_fee = 0.00

        try:
            credit_hours = float(item.get('credit_hours', 0.00))
        except ValueError:
            credit_hours = 0.00
        try:
            ceu_hours = float(item.get('ceu_hours', 0.00))
        except ValueError:
            ceu_hours = 0.00
        try:
            clock_hours = float(item.get('clock_hours', 0.00))
        except ValueError:
            clock_hours = 0.00
        try:
            load_hours = float(item.get('load_hours', 0.00))
        except ValueError:
            load_hours = 0.00

        section_data.append({
            'code': item.get('code'),
            'external_version_id': item.get('external_version_id'),
            'description': item.get('description'),
            'registration_url': item.get('registration_url'),
            'details_url': item.get('details_url'),
            'start_date': get_datetime_obj(item.get('start_date')),
            'end_date': get_datetime_obj(item.get('end_date')),
            'num_seats': num_seats,
            'available_seats': item.get('available_seats'),
            'is_active': item.get('is_active'),
            'execution_mode': item.get('execution_mode'),
            'execution_site': get_execution_site(item.get('execution_site'), course_provider_model),
            'registration_deadline': get_datetime_obj(item.get('registration_deadline')),
            'instructors': get_instructors(item.get('instructors', []), course_provider_model),
            'course_fee': {'amount': course_fee, 'currency': 'usd'},
            'credit_hours': credit_hours,
            'ceu_hours': ceu_hours,
            'clock_hours': clock_hours,
            'load_hours': load_hours,
            'schedules': get_schedules(item.get('schedules', []))
        })
    return section_data


def get_datetime_obj(date_time_str):
    if date_time_str is None or date_time_str == '':
        return None

    try:
        date_str, time_str = date_time_str.split('T')
    except ValueError:
        date_str = date_time_str.strip()
        time_str = '00:00:00'

    if time_str == '':
        time_str = '00:00:00'

    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    if time_str is None:
        time_str = '00:00:00'
    time = datetime.strptime(time_str, '%H:%M:%S').time()
    datetime_obj = datetime.combine(date, time)
    return datetime_obj


def prepare_course_postgres(course_model, course_provider):
    course_image_uri = None
    try:
        course_image_uri = course_model.image['original']
    except KeyError:
        pass
    course_data = {
        'course_provider': course_provider.id,
        'title': course_model.title,
        'slug': course_model.slug,
        'content_db_reference': str(course_model.id),
        'course_image_uri': course_image_uri,
        'content_ready': False,
        'external_image_url': None,
    }
    return course_data


def get_execution_site(data, course_provider_model):
    query = {'provider': course_provider_model.id, 'name': data}
    data = {
        '_cls': 'CourseProviderSite',
        'provider': course_provider_model.id,
        'name': data,
        'code': data
    }
    doc_id = upsert_mongo_doc(collection='course_provider_site', query=query, data=data)
    return doc_id


def prepare_section_postgres(data, fee, course, course_model):
    section_data = {
        'course': course.id,
        'name': data.get('code'),
        'fee': fee,
        'seat_capacity': data.get('num_seats'),
        'available_seat': data.get('available_seats'),
        'execution_mode': data.get('execution_mode', 'self-paced'),
        'registration_deadline': get_datetime_obj(data.get('registration_deadline')),
        'content_db_reference': str(course_model.id),
        'is_active': data.get('is_active', False),
        'start_date': get_datetime_obj(data.get('start_date')),
        'end_date': get_datetime_obj(data.get('end_date')),
        'execution_site': data.get('execution_site'),
    }

    return section_data


def translate_data(data, mapping):
    translated = []
    for item in data:
        item_data = {}
        for key, value in mapping.items():
            if key == 'name':
                name = item.get('first_name', '') + ' ' + item.get('middle_name', '') + ' ' + item.get('last_name', '')
                item_data[key] = name.replace('  ', ' ')  # replace two consecutive spaces with one
            elif key == 'section_type':
                item_data[key] = item.get('meeting_cde', 'LEC')
            elif key == 'start_at':
                item_data[key] = item.get('begin_dte', '') + 'T' + item.get('begin_tim', '')
            elif key == 'end_at':
                item_data[key] = item.get('end_dte', '') + 'T' + item.get('end_tim', '')
            elif key == 'instructors':
                item_data[key] = []  # translate_data(item['instructors'], value)
            elif key == 'schedules':
                item_data[key] = translate_data(item.get('schedules', []), value)

            elif key == 'execution_mode':
                item_data[key] = item.get(value, 'self-paced')

            elif key == 'is_active':
                item_data[key] = item.get(value, False)

            else:
                try:
                    item_data[key] = item[value]
                except KeyError:
                    item_data[key] = ''
        translated.append(item_data)
    return translated


def transale_j1_data(request_data):
    mapping = {
        'external_id': 'catalog_appid',
        'code': 'crs_cde',
        'title': 'crs_title',
        'slug': 'formatted_crs_cde',
        'description': 'catalog_text',
        'sections': {
            'code': 'section_appid',
            'external_version_id': 'external_version_id',
            'description': 'description',
            'registration_url': 'registration_url',
            'details_url': 'details_url',
            'start_date': 'first_begin_dte',
            'end_date': 'last_end_dte',
            'num_seats': 'open_seats',
            'available_seats': 'open_seats',
            'is_active': 'is_active',
            'execution_mode': 'execution_mode',
            'execution_site': 'execution_site',
            'registration_deadline': 'final_enrollment_dte',
            'instructors': {
                'name': ['first_name', 'middle_name', 'last_name'],
                'external_id': 'section_instructor_appid',
                'profile_urls': 'profile_urls',
                'image': 'image',
                'short_bio': 'short_bio',
                'detail_bio': 'detail_bio'
            },
            'course_fee': 'course_fees',
            'credit_hours': 'credit_hrs',
            'ceu_hours': 'ceu_hours',
            'clock_hours': 'crs_clock_hrs',
            'load_hours': 'load_hours',
            'schedules': {
                'section_type': ['meeting_cde', 'LEC'],
                'external_version_id': 'external_version_id',
                'name': 'name',
                'description': 'description',
                'start_at': ['begin_dte', 'begin_tim'],
                'end_at': ['end_dte', 'end_tim'],
                'building_name': 'building_description',
                'building_code': 'bldg_cde',
                'room_name': 'room_description',
            }
        },
    }

    data = {}

    for key, value in mapping.items():
        if key == 'sections':
            data[key] = translate_data(request_data.get('sections', []), value)
        else:
            data[key] = request_data[value]
    return data


def upsert_mongo_doc(collection=None, query=None, data=None):
    db = get_db()
    coll = db[collection]
    doc = coll.find_one(query)
    if doc is None:
        result = coll.insert_one(data)
        return result.inserted_id
    else:
        if collection == 'course':
            old_section_codes = [section['code'] for section in doc['sections']]
            new_section_codes = [section['code'] for section in data['sections']]
            sections = []

            for section in data['sections']:
                if section['code'] not in old_section_codes:
                    # this is new
                    sections.append(section)
                else:
                    # this is present in the old sections. so has to update
                    for old_section in doc['sections']:
                        if old_section['code'] == section['code']:
                            old_section.update(section)
                            sections.append(old_section)
            for section in doc['sections']:
                if section['code'] not in new_section_codes:
                    # this will not be updated. will remain as is.
                    sections.append(section)
            data['sections'] = sections
        if collection not in ['course_provider_site', 'instructor']:
            result = db.course.update_one(query, {'$set': data}, upsert=True)

        return doc['_id']
    return None


def insert_into_mongo(mongo_data, collection):
    db = get_db()
    coll = db[collection]
    result = coll.insert_one(mongo_data)

    return result.inserted_id


def get_data(doc_id, collection):
    db = get_db()
    coll = db[collection]
    doc = coll.find_one({'_id': ObjectId(doc_id)})
    return doc
