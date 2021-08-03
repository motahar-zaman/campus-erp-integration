from mongoengine import get_db
from config import mongo_client


def save_status_to_mongo(status_data=None, collection='EnrollmentStatusHistory'):
    print('logging status to mongo')

    try:
        mongo_client.connect_mongodb()

        db = get_db()
        coll = db.get_collection(collection)
        coll.insert_one(status_data)
    finally:
        mongo_client.disconnect_mongodb()
