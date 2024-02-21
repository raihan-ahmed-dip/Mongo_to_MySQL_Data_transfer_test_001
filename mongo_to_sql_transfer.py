from pymongo import MongoClient
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Atlas connection string
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')

# MySQL database connection details
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv( 'MYSQL_USER' )
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

def get_mongodb_data():
    # Connect to MongoDB Atlas
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client.ecook_iot
    collection = db.usage

    # Retrieve data from MongoDB
    data = list(collection.find())
    return data

def save_to_mysql(data):
    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = mysql_conn.cursor()


    # Insert data into MySQL table
    for entry in data:
        sn = entry['sn']
        rts = entry['rts']
        for dt_entry in entry['pd']['dt']:
            CDT = dt_entry['CDT']
            val = dt_entry['val']
            cursor.execute("INSERT IGNORE INTO ecook_data (sn, rts, CDT, val) VALUES (%s, %s, %s, %s)", (sn, rts, CDT, val))

    # Commit changes and close connections
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
    # Get data from MongoDB
    mongo_data = get_mongodb_data()

    # Save data to MySQL
    save_to_mysql(mongo_data)

