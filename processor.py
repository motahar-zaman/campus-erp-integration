import requests


def create_student(student, config):
    config['url'] = 'https://httpbin.org/post'
    resp = requests.request(config['method'], url=config['url'], data=student)
    if resp.status_code == 200:
        print(resp.json())
    else:
        print(resp.status_code)
    return resp.status_code


def create_enrollment(enrollment, config):
    config['url'] = 'https://httpbin.org/post'

    resp = requests.request(config['method'], url=config['url'], data=enrollment)

    if resp.status_code == 200:
        print(resp.json())
    else:
        print(resp.status_code)

    return resp.status_code
