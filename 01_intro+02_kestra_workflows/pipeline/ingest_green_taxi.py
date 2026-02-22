import pandas as pd
from sqlalchemy import create_engine

# Read the parquet file
df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Create engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Load to database
df.to_sql('green_taxi_trips', engine, if_exists='replace', index=False)

print(f"Loaded {len(df)} rows into green_taxi_trips table")
