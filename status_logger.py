from mongoengine import connect, disconnect, get_db
from decouple import config, UndefinedValueError


def save_status_to_mongo(status_data=None):
    try:
        mongodb_host = config('MONGODB_HOST')
        mongodb_database = config('MONGODB_DATABASE')
        mongodb_port = config('MONGODB_PORT')
        mongodb_username = config('MONGODB_USERNAME')
        mongodb_password = config('MONGODB_PASSWORD')
        mongodb_auth_database = config('MONGODB_AUTH_DATABASE')
    except UndefinedValueError:
        print('----> ', status_data)
        return

    disconnect()
    connect(mongodb_database, host=mongodb_host, port=int(mongodb_port), username=mongodb_username, password=mongodb_password, authentication_source=mongodb_auth_database)

    db = get_db()
    coll = db.get_collection('EnrollmentStatusHistory')
    coll.insert_one(status_data)
