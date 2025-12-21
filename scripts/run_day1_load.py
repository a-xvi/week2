from pathlib import Path
import pandas as pd
from bootcamp_data.config import make_paths
from bootcamp_data.transform import enforce_schema

ROOT = Path(__file__).resolve().parent.parent

print(make_paths(ROOT))



df= pd.read_csv("orders.csv")
print(df)

df = enforce_schema(df)
print("\n after enforce\n")
print(df)
