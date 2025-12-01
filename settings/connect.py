import os
from dotenv import load_dotenv

from binance.spot import Spot

import mysql.connector

from sqlalchemy import create_engine
from urllib.parse import quote_plus


load_dotenv()


# Connect to Binance API
def binance_client(testnet: bool = False):
    if not testnet:
        return Spot(api_key=os.getenv('BINANCE_API_KEY'),
                    api_secret=os.getenv('BINANCE_API_SECRET'),
                    base_url='https://api.binance.com')
    elif testnet:
        return Spot(api_key=os.getenv('BINANCE_API_KEY_TEST'),
                    api_secret=os.getenv('BINANCE_API_SECRET_TEST'),
                    base_url='https://testnet.binance.vision')
    else:
        ValueError('Invalid testnet value. Must be True or False.')


# Connect to MySQL database
def mysql_db_connection():
    return mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE'),
        port=int(os.getenv('MYSQL_PORT'))
    )


# Build SQLAlchemy engine for MySQL
def sqlalchemy_create_engine():
    host = os.getenv('MYSQL_HOST')
    user = os.getenv('MYSQL_USER')
    dbname = os.getenv('MYSQL_DB')
    port = int(os.getenv('MYSQL_PORT'))
    raw_pw = os.getenv('MYSQL_PASSWORD')
    pw = quote_plus(raw_pw)
    uri = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{dbname}"
    return create_engine(uri)
