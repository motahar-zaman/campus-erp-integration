from decouple import config
import os


class Config(object):
    # Mongo
    MONGODB_HOST = config('MONGODB_HOST')
    MONGODB_PORT = int(config('MONGODB_PORT'))
    MONGODB_AUTH_DATABASE = config('MONGODB_AUTH_DATABASE')
    MONGODB_DATABASE = config('MONGODB_DATABASE')
    MONGODB_USERNAME = config('MONGODB_USERNAME')
    MONGODB_PASSWORD = config('MONGODB_PASSWORD')
