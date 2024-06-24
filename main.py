from contextlib import asynccontextmanager
from typing import AsyncIterator
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import find_dotenv, load_dotenv
from starlette.responses import JSONResponse
from DB.db_connection import databaseConnection
from algorithms.cbfiltering import ProductRecommender
from pre_processing import data_pre_cbfilter
import os
import nltk
from fastapi_cache.decorator import cache
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend


# Load dotenv
load_dotenv(find_dotenv(".env"))
PORT = str(os.getenv("PORT"))
FRONTEND_URL = str(os.getenv("FRONTEND_URL"))
CB_FILTER_DATA_COLLECTION_NAME = str(os.getenv("CB_FILTER_COLLECTION_NAME"))

# Global db map
db_map = {"client": None, "db": None}


async def get_all(db):
    data = []
    async for doc in db[CB_FILTER_DATA_COLLECTION_NAME].find({}):
        data.append(doc)
    return data


# Lifespan startup and shutdown
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Connect to MongoDB
    get_client = databaseConnection().connect()
    client = get_client[0]
    db = get_client[1]
    db_map["client"] = client
    db_map["db"] = db

    # Download nltk
    nltk.download("punkt")
    nltk.download("stopwords")
    nltk.download("wordnet")

    # fastapi caching
    FastAPICache.init(backend=InMemoryBackend(), prefix="fastapi-cache")

    yield

    # Close MongoDB connection
    databaseConnection().disconnect()


# Initialize FastAPI app
app = FastAPI(
    title="Ecommerce Recommendation Engine",
    description="Fast API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
origins = [str(FRONTEND_URL)]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
@cache(expire=60)
async def insert_product_to_dataset(background_tasks: BackgroundTasks):
    background_tasks.add_task(data_pre_cbfilter.generate_json, db_map["db"])
    return JSONResponse(
        {
            "data": "Updating db with new product data in background! Awaiting model training",
            "err": None,
        }
    )


@app.get("/api/cbfiltering")
@cache(expire=300)
async def get_recommendations(product_id: str, num_recommendations: int):
    df = await get_all(db_map["db"])
    recommender = ProductRecommender(db_map["db"], df)
    try:
        recommendations = await recommender.generate_recommendations(
            product_id, num_recommendations
        )
        return JSONResponse({"data": recommendations, "err": None})
    except Exception as e:
        print(str(e))
        return JSONResponse(
            {"data": None, "err": "No data has been trained till date!"}
        )
