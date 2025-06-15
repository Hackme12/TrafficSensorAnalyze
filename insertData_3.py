import pandas as pd
from sqlalchemy import create_engine

# MySQL connection
password = 'Bigdata123'  # replace with your real password
engine = create_engine(f"mysql+mysqlconnector://root:{password}@localhost:3306/traffic_analytics")

# Load main CSV
df = pd.read_csv("data/all_features_traffic_dataset.csv")
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

# --- Load dimension tables from DB for key mapping ---
dim_time = pd.read_sql("SELECT * FROM dim_time", engine)
dim_weather = pd.read_sql("SELECT * FROM dim_weather", engine)
dim_incident = pd.read_sql("SELECT * FROM dim_incident", engine)
dim_location = pd.read_sql("SELECT * FROM dim_location", engine)
dim_congestion = pd.read_sql("SELECT * FROM dim_congestion", engine)
fact_traffic = pd.read_sql("SELECT * FROM fact_traffic", engine)

# Check actual column names in dim_location
print(dim_location.columns)


# --- Join on timestamp, weather, incident, location, congestion ---
df['hour'] = df['Timestamp'].dt.hour
df['day_of_week'] = df['Timestamp'].dt.dayofweek

# Merge for time_id
df = df.merge(dim_time, how='left',
              left_on=['Timestamp', 'hour', 'day_of_week'],
              right_on=['timestamp', 'hour', 'day_of_week'])
df.rename(columns={'time_id': 'fk_time'}, inplace=True)

# Merge for weather_id
df = df.merge(dim_weather, how='left',
              left_on='Weather_Conditions', right_on='weather_condition')
df.rename(columns={'weather_id': 'fk_weather'}, inplace=True)

# Merge for incident_id
df = df.merge(dim_incident, how='left',
              left_on='Traffic_Incidents', right_on='incident_type')
df.rename(columns={'incident_id': 'fk_incident'}, inplace=True)

# Merge for location_id
df = df.merge(dim_location, how='left',
              left_on=['Number_of_Lanes', 'Population_Density'],
              right_on=['number_of_lanes', 'population_density'])
df.rename(columns={'location_id': 'fk_location'}, inplace=True)

# Merge for congestion_id
df = df.merge(dim_congestion, how='left',
              left_on='Congestion_Level', right_on='congestion_level')
df.rename(columns={'congestion_id': 'fk_congestion'}, inplace=True)

print(fact_traffic.columns)

# --- Prepare fact_traffic DataFrame ---
fact_df = df[['fk_time', 'fk_weather', 'fk_incident', 'fk_location', 'fk_congestion',
              'Traffic_Volume', 'Traffic_Speed', 'Traffic_Density', 'Travel_Time',
              'Delay_Reduction', 'Emission_Levels', 'Queue_Length_Reduction']].copy()

# Rename foreign keys to match DB column names
fact_df.rename(columns={
    'fk_time': 'time_id',
    'fk_weather': 'weather_id',
    'fk_incident': 'incident_id',
    'fk_location': 'location_id',
    'fk_congestion': 'congestion_id'
}, inplace=True)

fact_df.dropna(inplace=True)  # Ensure all FK fields are valid

print(fact_traffic.columns)

# --- Insert into fact_traffic ---
fact_df.to_sql('fact_traffic', con=engine, if_exists='append', index=False)

print("✅ fact_traffic table successfully populated.")
