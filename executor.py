from processors.mindedge import MindEdgeProcessor

configs = {
    'demo': {
        'processor_class': '',
        'credentials': {
            'test_url': '',
            'production_url': '',
            'username': '',
            'password': '',
            'token': '',
        }
    },

    'mindedge': {
        'processor_class': MindEdgeProcessor,
        'credentials': {
            'test_url': 'https://api.mindedgeuniversity.com/v1/studentService',
            'production_url': 'https://api.mindedgeuniversity.com/v1/studentService',
            'username': '',
            'password': '',
            'token': '',
            'url': 'https://api.mindedgeuniversity.com/v1/studentService',
        }
    }
}


def execute(message_data):
    # message_data = {'data': serializer.data, 'metadata': {'action': 'create_student', 'erp': 'mindedge', 'user': user}}
    data = message_data['data']

    metadata = message_data['metadata']
    erp = metadata['erp']
    user = metadata['user']

    config = configs[erp]
    processor_class = config['processor_class']
    credentials = config['credentials']

    processor_obj = processor_class(credentials, user, data)
    action = getattr(processor_obj, metadata['action'])
    action()
