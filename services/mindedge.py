import requests


class MindEdgeService():
    '''
    Inpmplements mindedge's api
    '''

    def __init__(self, credentials, profile, data):
        # api credentials
        self.username = credentials['username']
        self.password = credentials['password']
        self.token = credentials['token']
        self.url = credentials['url']

        # current profile
        self.profile = profile

        # data. it could be a course, or a suit of courses
        self.data = data

    def authenticate(self):
        payload = {'username': self.username, 'password': self.password, 'token': self.token}
        response = requests.post(self.url, json=payload)
        resp = response.json()

        if resp['status'] == 'success':
            self.auth_header = {'Authorization': resp['access_token']}
            return True
        else:
            # Raise exception here
            return False

    def enroll(self):
        payload = {
            'action': 'enroll',
            'email': self.profile['primary_email'],
            'first_name': self.profile['first_name'],
            'last_name': self.profile['last_name'],
            'login_link': True
        }

        if 'cid' in self.data.keys():
            payload['cid'] = self.data['cid']
        else:
            payload['sid'] = self.data['sid']

        response = requests.post(self.url, json=payload, headers=self.auth_header)

        resp = response.json()

        if resp['status'] == 'fail':
            if resp['error'].lower() == 'Student already enrolled in course'.lower():
                resp = self.get_tokenized_url()
                resp['already_enrolled'] = True

        return resp

    def find(self):
        '''
        Checks if a profile is present in mindedge db by their email address. returns user data if present
        '''
        payload = {'action': 'find', 'email': self.profile['primary_email']}
        response = requests.post(self.url, json=payload, headers=self.auth_header)

        return response.json()

    def check_enrollment(self):
        '''
        Checks if passed profile is enrolled in the passed course. returns true or false
        '''

        payload = {
            'action': 'checkEnrollment',
            'email': self.profile['primary_email'],
            'cid': self.data['cid']
        }

        response = requests.post(self.url, json=payload, headers=self.auth_header)

        return response.json()

    def get_tokenized_url(self):
        payload = {
            'action': 'getAccountlessSSO',
            'email': self.profile['primary_email'],
            'first_name': self.profile['first_name'],
            'last_name': self.profile['last_name'],
            'login_link': True
        }

        if 'cid' in self.data.keys():
            payload['cid'] = self.data['cid']
        else:
            payload['sid'] = self.data['sid']

        response = requests.post(self.url, json=payload, headers=self.auth_header)

        return response.json()
