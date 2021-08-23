import mongoengine
import datetime
from mongoengine import get_db

def save_status_to_mongo(status_data=None, collection='EnrollmentStatusHistory'):
    print('logging status to mongo')

    try:
        db = get_db()
        coll = db.get_collection(collection)
        status_data['datetime'] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        coll.insert_one(status_data)
    except mongoengine.connection.ConnectionFailure:
        pass