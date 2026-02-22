import pandas as pd
from sqlalchemy import create_engine

# Read the CSV file
df = pd.read_csv('taxi_zone_lookup.csv')

# Create engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Load to database
df.to_sql('taxi_zones', engine, if_exists='replace', index=False)

print(f"Loaded {len(df)} rows into taxi_zones table")
