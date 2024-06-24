import motor.motor_asyncio
from dotenv import find_dotenv, load_dotenv
import os


# Load dotenv
load_dotenv(find_dotenv(".env"))

# Mongodb database and collection
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")


# MongoDBConnection
class databaseConnection:
    def __init__(self) -> None:
        self.DB_URL = str(os.getenv("DATABASE_URL"))

    def connect(self):
        client = motor.motor_asyncio.AsyncIOMotorClient(self.DB_URL)
        db = client[str(MONGODB_DATABASE)]
        print("Connected to mongodb instance!")
        return [client, db]

    def disconnect(self):
        print("Disconnected from mongodb instance!")
