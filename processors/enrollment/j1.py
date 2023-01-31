import json
from multiprocessing.sharedctypes import Value
import requests
from campuslibs.loggers.mongo import save_to_mongo
from decouple import config
import time
from django.utils import timezone
import pika
from shared_models.models import CourseEnrollment


def handle_enrollment(data, configuration, payload, ch, method, properties):
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')

    exchange_dead_letter = 'dlx'
    routing_key = 'enrollment.enroll'
    resp_headers = {}
    resp_status_code = 200

    if configuration.get('auth_type', '') == 'basic':
        try:
            response = requests.request(
                "POST",
                configuration.get('enrollment_url', ''),
                auth=(
                    configuration.get('username', ''),
                    configuration.get('password', '')
                ),
                headers=headers,
                data=json.dumps(data)
            )
            response.raise_for_status()
            resp_headers = response.headers
            resp_status_code = response.status_code
        except requests.exceptions.RequestException as err:
            payload['retry_count'] += 1
            if payload['retry_count'] <= int(config('TASK_MAX_RETRY_COUNT')):
                payload['next_request_time'] = str(
                    timezone.now() + timezone.timedelta(seconds=payload['retry_count'] * int(config('RETRY_INTERVAL'))))
                ch.basic_publish(exchange='campusmq', routing_key='enrollment.enroll', body=json.dumps(payload))
            else:
                ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))

            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            store_logging_data(data, headers, resp_headers, str(err), status_code=503)
            return {'message': str(err)}

    else:
        try:
            response = requests.request(
                "POST",
                configuration.get('enrollment_url', ''),
                headers=headers,
                data=json.dumps(data)
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            payload['retry_count'] += 1
            if payload['retry_count'] <= int(config('TASK_MAX_RETRY_COUNT')):
                payload['next_request_time'] = str(timezone.now() + timezone.timedelta(seconds = payload['retry_count'] * int(config('RETRY_INTERVAL'))))
                ch.basic_publish(exchange='campusmq', routing_key='enrollment.enroll', body=json.dumps(payload))
            else:
                ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))

            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            store_logging_data(data, headers, resp_headers, str(err), status_code=503)
            return {'message': str(err)}
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)

    resp = {}
    try:
        resp = response.json()
    except ValueError:
        resp = {'message': 'invalid response received'}
        save_to_mongo(data={'erp': 'j1:response', 'data': {'message': 'invalid response received'}}, collection='erp_response')
        store_logging_data(data, headers, resp_headers, resp, resp_status_code)
        return resp
    store_logging_data(data, headers, resp_headers, resp, resp_status_code)
    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': resp}}, collection='erp_response')
    return resp


def handle_enrollment_cancellation(data, configuration, payload, ch, method, properties):
    headers = {
        'Content-Type': 'application/json'
    }
    save_to_mongo(data={'erp': 'j1:payload', 'data': data}, collection='erp_response')

    exchange_dead_letter = 'dlx'
    routing_key = 'enrollment.cancel'
    resp_headers = {}
    resp_status_code = 200

    if configuration.get('auth_type', '') == 'basic':
        try:
            response = requests.request(
                "POST",
                configuration.get('enrollment_cancel_url', 'https://6327e8bc5731f3db99603a82.mockapi.io/enrollment/cancel-enrollment'),
                auth=(
                    configuration.get('username', ''),
                    configuration.get('password', '')
                ),
                headers=headers,
                data=json.dumps(data)
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            payload['retry_count'] += 1
            if payload['retry_count'] <= int(config('TASK_MAX_RETRY_COUNT')):
                payload['next_request_time'] = str(
                    timezone.now() + timezone.timedelta(seconds=payload['retry_count'] * int(config('RETRY_INTERVAL'))))
                ch.basic_publish(exchange='campusmq', routing_key='enrollment.enroll', body=json.dumps(payload))
            else:
                ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))

            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            store_logging_data(data, headers, resp_headers, str(err), status_code=503)
            return {'message': str(err)}

    else:
        try:
            response = requests.request(
                "POST",
                configuration.get('enrollment_cancel_url', 'https://6327e8bc5731f3db99603a82.mockapi.io/enrollment/cancel-enrollment'),
                headers=headers,
                data=json.dumps(data)
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            payload['retry_count'] += 1
            if payload['retry_count'] <= int(config('TASK_MAX_RETRY_COUNT')):
                payload['next_request_time'] = str(timezone.now() + timezone.timedelta(seconds = payload['retry_count'] * int(config('RETRY_INTERVAL'))))
                ch.basic_publish(exchange='campusmq', routing_key='enrollment.enroll', body=json.dumps(payload))
            else:
                ch.basic_publish(exchange=exchange_dead_letter, routing_key=routing_key, body=json.dumps(payload))

            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            save_to_mongo(data={'erp': 'j1:response', 'data': {'message': str(err)}}, collection='erp_response')
            store_logging_data(data, headers, resp_headers, str(err), status_code=503)
            return {'message': str(err)}
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)

    resp = {}
    try:
        resp = response.json()
    except ValueError:
        resp = {'message': 'invalid response received'}
        save_to_mongo(data={'erp': 'j1:response', 'data': {'message': 'invalid response received'}}, collection='erp_response')
        store_logging_data(data, headers, resp_headers, resp, resp_status_code)
        return resp
    store_logging_data(data, headers, resp_headers, resp, resp_status_code)
    save_to_mongo(data={'erp': 'j1:response', 'data': {'message': resp}}, collection='erp_response')
    return resp


def store_logging_data(request_body, request_headers, response_headers, response_body, status_code=200):
    try:
        enrollment_id = request_body['data']['enrollments']['enrollment_id']
        course_enrollment = CourseEnrollment.objects.get(ref_id=enrollment_id)
        log_data = {
            'course_provider': {
                'id': str(course_enrollment.course.course_provider.id),
                'name': course_enrollment.course.course_provider.name,
            },
            'data': {
                'request': {
                    'headers': request_headers,
                    'body': request_body
                },
                'response': {
                    'headers': response_headers,
                    'body': response_body
                }
            },
            'status_code': status_code,
            'summary': 'request-response of enrollment to partner',
            'ERP': course_enrollment.course.course_provider.configuration.get('erp', ''),
            'created_at': timezone.now()
        }

        save_to_mongo(data=log_data, collection='erp_request_response')
    except:
        pass
