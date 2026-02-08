import sys
import pandas as pd

#script 1
print("arguments", sys.argv)

month = int(sys.argv[1])

print(f"Running pipeline for month={month}")

#script 2
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

