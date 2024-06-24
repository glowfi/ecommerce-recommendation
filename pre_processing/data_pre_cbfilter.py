from bson import DBRef, ObjectId
from dotenv import find_dotenv, load_dotenv
import os


# Load dotenv
load_dotenv(find_dotenv(".env"))

CB_FILTER_DATA_COLLECTION_NAME = os.getenv("CB_FILTER_COLLECTION_NAME")
CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME = os.getenv(
    "CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME"
)


def snake_to_camel(s):
    text = s.title().replace("_", "")
    return text[0].lower() + text[1:]


async def get_ans(db, collection_name, id=None, isFirst=False):
    query_dict = {}
    data = []

    if isFirst and id:
        query_dict = {
            "_id": {"$gt": ObjectId(id) if not isinstance(id, ObjectId) else id}
        }

    elif id:
        query_dict = {"_id": ObjectId(id) if not isinstance(id, ObjectId) else id}

    async for doc in db[collection_name].find(query_dict):
        tmp = {}
        for key in doc:
            if isinstance(doc[key], DBRef):
                collection_name = doc[key].collection
                id = doc[key].id
                if key.find("_") != -1:
                    new_keyname = snake_to_camel(key)
                else:
                    new_keyname = key

                tmp[new_keyname] = await get_ans(db, collection_name, id, False)

            elif isinstance(doc[key], ObjectId):
                tmp[key] = str(doc[key])
            else:
                if key.find("_") != -1:
                    new_keyname = snake_to_camel(key)
                else:
                    new_keyname = key
                tmp[new_keyname] = doc[key]

        data.append(tmp)
    return data


async def handle_last_known_id(products, db):
    data = {"lastknowId": str(products[-1]["_id"])}

    currData = (
        await db[CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME].find({}).to_list(1)
    )

    if currData:
        await db[CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME].replace_one(
            {"_id": currData[-1].id}, data
        )
    else:
        await db[CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME].insert_one(data)


async def dump_json(db, products, location=None):

    await db[CB_FILTER_DATA_COLLECTION_NAME if not location else location].insert_many(
        products
    )


async def generate_json(db):
    # If last know idx present then analyze data from after that id
    last_idx = (
        await db[CB_FILTER_LAST_SEEN_PRODUCT_INDEX_COLLECTION_NAME].find({}).to_list(1)
    )
    if last_idx:
        print("Already have some data!")

        # Load last seen product index
        data = last_idx[-1]
        id = data["lastknowId"]

        # Get new products
        new_products = await get_ans(db, "Product", id, True)

        # Append it with existing dataset
        if new_products:
            print("Appending")

            # Dump Json and product_lookup
            await dump_json(db, new_products)

            # Update last seen product
            await handle_last_known_id(new_products, db)
    else:
        print("Seeing data first time")

        # Get products
        products = await get_ans(db, "Product", None, True)

        # Dump Json and product_lookup
        await dump_json(db, products, None)
        # await generate_product_lookup_dict(products, True)

        # Update last seen product
        await handle_last_known_id(products, db)
