import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from base_scraper import Scraper

load_dotenv()


def query_generator():
    lst = list(pd.read_csv("data/queries.csv")["query"])
    for query in lst:
        yield query


def query_scraper(query: str):
    headers = {"Ocp-Apim-Subscription-Key": os.getenv("KEY_1")}
    params = {"q": query, "textDecorations": True, "textFormat": "HTML"}

    response = requests.get(os.getenv("ENDPOINT"), headers=headers, params=params)
    response.raise_for_status()
    search_results = response.json()

    for i in search_results["webPages"]["value"]:
        yield {"query": query, "json": i}

    print(f"Scraping {query} done")
    time.sleep(0.01)


def search_result_scrape():
    dummy_data = {"query": "", "json": ""}

    s = Scraper(
        file_name="data/bing_results.db",
        num_workers=10,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        timeout=30,
        cmt_interval=1
    )
    s.start_scraping()


if __name__ == "__main__":
    search_result_scrape()
