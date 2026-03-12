from pymongo import MongoClient
from sqlalchemy import create_engine
from .config import MONGO_URI, POSTGRES_URI, DB_NAME


def get_mongo():
    return MongoClient(MONGO_URI)[DB_NAME]


def get_pg():

    engine = create_engine(POSTGRES_URI)

    return engine.raw_connection()