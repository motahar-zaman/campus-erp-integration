from mongoengine import connect, disconnect, get_db

def save_status_history(status_data=None):
    username = config('MONGO_USERNAME', 'cc-dev-admin-api')
    password = config('MONGO_PASSWORD', 'FWPCIvc7McXRhg')
    auth_source = config('MONGO_AUTH_SOURCE', 'admin')
    host = config('MONGO_HOST', 'ec2-18-188-170-233.us-east-2.compute.amazonaws.com')
    port = config('MONGO_PORT', '27017')
    db_name = config('MONGO_DB_NAME', 'campus')

    disconnect()
    connect(db_name, host=host, port=int(port), username=username, password=password, authentication_source=auth_source)
    db = get_db()
    coll = db.get_collection('EnrollmentStatusHistory')
    coll.insert_one(status_data)
