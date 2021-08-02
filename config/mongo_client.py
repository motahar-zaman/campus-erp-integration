import mongoengine

from config.config import Config


def connect_mongodb():
    try:
        connection = mongoengine.connect(
            Config.MONGODB_DATABASE,
            host=Config.MONGODB_HOST,
            port=Config.MONGODB_PORT,
            username=Config.MONGODB_USERNAME,
            password=Config.MONGODB_PASSWORD,
            authentication_source=Config.MONGODB_AUTH_DATABASE
        )
        return connection
    except Exception as e:
        raise e


def disconnect_mongodb():
    try:
        mongoengine.disconnect()
    except Exception as e:
        pass
