import ast
import sqlite3
import uuid

import pandas as pd

con = sqlite3.connect("data/bing_results.db")
df = pd.read_sql_query("SELECT * from main", con)

df['url'] = df.apply(lambda x: ast.literal_eval(x["json"])["url"], axis=1)
df['id'] = df.apply(lambda x: uuid.uuid4(), axis=1)
for key in ["kharagpur", "west-bengal", "paschim-medinipur", "iit-kharagpur", "iitkgp.ac.in"]:
    df = df[~df["url"].str.lower().str.contains(key)]
df = df.drop(columns=["json"])
df.to_csv("data/webpages.csv", index=False)
