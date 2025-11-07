import os
import pandas as pd
OUTPUT_CSV = "gpkg_cardinality_report.csv"
filepath = os.path.join(r'c:\nardata\localcode\data-exchange-scripts', OUTPUT_CSV)
df = pd.read_csv(filepath)
df.head()
