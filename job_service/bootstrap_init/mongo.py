import secret
import pandas as pd
import bootstrap_init
from pymongo import MongoClient 

from global_state import global_instance

def fetchUniqueCompanyList() -> None:
    """
    Doc String
    """
    logger = bootstrap_init.logger
    x = {"urls":[]}

    try:
        logger.info("Fetching Company Unique List...")
        client = MongoClient(secret.mongo_uri)
        db = client[secret.mongo_db]
        collection = db[secret.mongo_collection_1]

        all_documents = collection.find()

        for document in all_documents:
            x["urls"].append(document["url"])
        df = pd.DataFrame(x)
        global_instance.modify_data("company_urls", df)
        logger.info("Done!")
        print(df)

        return x
    except Exception as e:
        logger.error("Unable to fetch company list!")
        raise Exception(e)