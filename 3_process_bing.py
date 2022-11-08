import json
import sqlite3

import pandas as pd

con = sqlite3.connect("data/bing_results.db")
df = pd.read_sql_query("SELECT * from main", con)
print(df)
df['url'] = df.apply(lambda x: json.loads(x['json']), axis=0)
print(df)
