import requests


class MindEdgeProcessor():
    '''
    Inpmplements mindedge's api
    '''

    def __init__(self, credentials, student, data):
        # api credentials
        self.username = credentials['username']
        self.password = credentials['password']
        self.token = credentials['token']
        self.url = credentials['url']

        # current student
        self.student = student

        # data. it could be a course, or a suit of courses
        self.data = data

    def authenticate(self):
        payload = {'username': self.username, 'password': self.password, 'token': self.token}
        response = requests.post(self.url, json=payload)

        if response.status_code == 200:
            resp = response.json()
            # this should contain something like {'status': '', 'access_token': ''}
            # we set a the authorization header in a variale for later use
            self.auth_header = {'Authorization': 'Authorization {}'.format(resp['access_token'])}

            return True
        else:
            # Raise exception here
            return False

    def enroll(self):
        payload = {'action': 'enroll', 'email': self.student['email'], 'first_name': self.student['first_name'], 'last_name': self.student['last_name'], 'cid': self.data['mindedge_id']}
        resp = requests.post(self.url, json=payload, header=self.auth_header)

        # it's not clear what status code means success. it's not clear if status code is the indicator or a message inside
        # response data. it will have to be examined.
        # however, the success response will look like this: {'status': 'success', 'message': 'Student successfully enrolled in course.', 'login_link': 'https://...'}
        # and failure case will have this: {'status': 'fail': 'error': API access error. Please check bla bla'}
        resp_data = resp.json()
        if ['status'] == 'success':
            return resp_data['login_link']
        else:
            pass
            # raise resp_data['error']

    def find(self):
        '''
        Checks if a student is present in mindedge db by their email address. returns user data if present
        '''
        payload = {'action': 'find', 'email': self.student['email']}
        resp = requests.post(self.url, json=payload, header=self.auth_header)

        resp_data = resp.json()

        if resp_data['status'] == 'success':
            return resp_data

        return 'Cournd not find user'
        # raise a mild exception

    def check_enrollment(self):
        '''
        Checks if passed student is enrolled in the passed course. returns true or false
        '''
        payload = {'action': 'checkEnrollment', 'email': self.student['email'], 'cid': self.data['mindedge_id']}
        resp = requests.post(self.url, json=payload, header=self.auth_header)
        resp_data = resp.json()

        if resp_data['status'] == 'success':
            return resp_data['enrolled']
        return 'invalid req'
        # raise exception
