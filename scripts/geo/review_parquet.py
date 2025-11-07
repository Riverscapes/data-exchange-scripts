"""check what's going on"""

import os
import pyarrow.parquet as pq

folder = r"C:\nardata\work\rme_extraction\rme-athena\downloads\1012010302"
folder = r"C:\nardata\localcode\data-exchange-scripts\inputs"
schemas = {}

for filename in os.listdir(folder):
    if filename.endswith(".parquet"):
        path = os.path.join(folder, filename)
        schema = pq.read_schema(path)
        schemas[filename] = schema

# Compare schemas
for fname, schema in schemas.items():
    print(f"\n{fname} schema:")
    print(schema)
