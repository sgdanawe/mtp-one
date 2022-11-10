import requests
from bs4 import BeautifulSoup
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from base_scraper import Scraper

load_dotenv()


def query_generator():
    lst = pd.read_csv("data/webpages.csv").to_dict(orient="records")
    for query in lst:
        yield query


def query_scraper(row):
    print(row)
    html = requests.get(row["url"], timeout=5).content
    text = '\n'.join(i for i in BeautifulSoup(html, "html.parser").stripped_strings if len(i) > 100)

    """
    soup = BeautifulSoup(html, features="html.parser")

    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    print(text)

    lines = (line.strip() for line in text.splitlines() if len(line) > 200)
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n\n'.join(chunk for chunk in chunks if chunk)
    """
    with open(f"./data/webpages/{row['id']}.txt", "w+") as f:
        f.write(text)
    yield {"id": row["id"]}


def search_result_scrape():
    dummy_data = {"id": ""}

    s = Scraper(
        file_name="data/webpages_scraped.db",
        num_workers=10,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        timeout=300,
        cmt_interval=1
    )
    s.start_scraping()


if __name__ == "__main__":
    search_result_scrape()
