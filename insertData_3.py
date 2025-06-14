import pandas as pd
from sqlalchemy import create_engine

# Replace with your MySQL root password
password = 'Bigdata123'  # <-- Update this
db_name = 'traffic_analytics'

# Connect to MySQL
engine = create_engine(f"mysql+mysqlconnector://root:{password}@localhost:3306/{db_name}")

# Load CSV
csv_path = "data/all_features_traffic_dataset.csv"  # Update path if needed
df = pd.read_csv(csv_path)

# Parse timestamp corr
# ectly
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

# ---------- Load dim_time ----------
dim_time = df[['Timestamp']].drop_duplicates().copy()
dim_time['hour'] = dim_time['Timestamp'].dt.hour
dim_time['day_of_week'] = dim_time['Timestamp'].dt.dayofweek
dim_time = dim_time[['Timestamp', 'hour', 'day_of_week']]

# Insert into dim_time
dim_time.to_sql('dim_time', con=engine, if_exists='append', index=False)

# ---------- Load dim_location ----------
dim_location = df[['Number_of_Lanes', 'Population_Density']].drop_duplicates()

# Insert into dim_location
dim_location.to_sql('dim_location', con=engine, if_exists='append', index=False)

print("✅ dim_time and dim_location successfully populated.")
