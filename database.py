from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConfigurationError
from os import environ
from logging import info

from _init_ import start_app

#
start_app()

MONGO_DB_URI = environ.get('MONGO_DB_URI')
MONGO_DB_PASSWORD = environ.get('MONGO_DB_PASSWORD')
URI = MONGO_DB_URI.replace('<password>', MONGO_DB_PASSWORD)


async def get_mongo_client():
    try:
        # Create a new client and connect to the server
        client = MongoClient(URI, server_api=ServerApi('1'))
        return True, client
    except ConfigurationError as e:
        info(e)
        return False, e


async def variables():
    return environ.get('MONGO_DB_APPNAME')


async def ping_db() -> str:
    client_check, client = await get_mongo_client()

    # When client is live
    if client_check:
        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            info("Pinged your deployment. You successfully connected to MongoDB!")
            return 'pong!'
        except Exception as e:
            info(e)
            return 'failed'

    return client


async def insert_record(database_name: str, collection_name: str, record: dict):
    _, client = await get_mongo_client()
    # Connect to database
    info('DB name is {}'.format(database_name))
    database = client[database_name]
    # Access collection
    collection = database[collection_name]
    # Insert record
    response = collection.insert_one(record)

    record_id = str(response.inserted_id)

    return record_id


async def get_record(database_name: str, collection_name: str, filter_query: dict):
    _, client = await get_mongo_client()

    database = client[database_name]

    collection = database[collection_name]

    response = collection.find_one(filter_query)

    return response
