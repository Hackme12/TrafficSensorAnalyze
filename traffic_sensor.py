from pyspark.sql import SparkSession
from insertData_3 import fact_traffic
import matplotlib.pyplot as plt

# 1. Start Spark Session
spark = (SparkSession.builder.appName("TrafficSensorStream")
          #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
          .config("spark.jars", "/Users/pranaya/Desktop/mysql-connector-j-9.3.0.jar")
         .getOrCreate())

jdbc_url = "jdbc:mysql://localhost:3306/traffic_analytics?serverTimezone=UTC&useLegacyDatetimeCode=false"
connection_props = {
    "user": "root",
    "password": "Bigdata123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load tables
fact_df = spark.read.jdbc(jdbc_url, "fact_traffic", properties=connection_props)
dim_time = spark.read.jdbc(jdbc_url, "dim_time", properties=connection_props)
dim_weather = spark.read.jdbc(jdbc_url, "dim_weather", properties=connection_props)
dim_incident = spark.read.jdbc(jdbc_url, "dim_incident", properties=connection_props)
dim_location = spark.read.jdbc(jdbc_url, "dim_location", properties=connection_props)
dim_congestion = spark.read.jdbc(jdbc_url, "dim_congestion", properties=connection_props)

# Register as temp views
fact_df.createOrReplaceTempView("fact_traffic")
dim_time.createOrReplaceTempView("dim_time")
dim_weather.createOrReplaceTempView("dim_weather")
dim_incident.createOrReplaceTempView("dim_incident")
dim_location.createOrReplaceTempView("dim_location")
dim_congestion.createOrReplaceTempView("dim_congestion")


fact_traffic_sql_query = """
SELECT
    ft.traffic_id,
    
    -- Time info
    dt.timestamp,
    dt.hour,
    CASE 
        WHEN dt.hour BETWEEN 7 AND 9 THEN 'Morning Peak'
        WHEN dt.hour BETWEEN 16 AND 18 THEN 'Evening Peak'
        ELSE 'Off Peak'
    END AS peak_category,
    dt.day_of_week,
    
    -- Weather & Incident
    dw.weather_condition,
    di.incident_type,
    
    -- Location attributes
    dl.number_of_lanes,
    dl.population_density,
    
    -- Congestion level (dimensional label)
    dc.congestion_level,
    
    -- Metrics from fact table
    ft.traffic_volume,
    ft.traffic_speed,
    ft.traffic_density,
    ft.travel_time,
    ft.delay_reduction,
    ft.queue_length_reduction,
    ft.emission_levels,

    -- Derived metrics
    ROUND(ft.traffic_volume / NULLIF(ft.traffic_density, 0), 2) AS vehicles_per_unit_density,
    ROUND(ft.emission_levels * ft.traffic_volume, 2) AS total_emissions_estimate

FROM fact_traffic ft
JOIN dim_time dt ON ft.time_id = dt.time_id
JOIN dim_weather dw ON ft.weather_id = dw.weather_id
JOIN dim_incident di ON ft.incident_id = di.incident_id
JOIN dim_location dl ON ft.location_id = dl.location_id
JOIN dim_congestion dc ON ft.congestion_id = dc.congestion_id

WHERE dt.timestamp IS NOT NULL AND  ft.traffic_id <= 61000
ORDER BY dt.timestamp
"""


fact_traffic_1 = spark.sql(fact_traffic_sql_query).toPandas()
fact_traffic_1.to_csv("data/test.csv", index=False)

# data cleanup
# Null summary
# fact_traffic_1.select([count(when(col(c).isNull(), c)).alias(c) for c in fact_traffic_1.columns]).show()

# Drop rows with nulls in key fields (customize as needed)
key_fields = ['timestamp', 'traffic_volume', 'traffic_speed', 'traffic_density']
df_clean = fact_traffic_1.dropna(subset=key_fields)



print(df_clean.value_counts())


# weather by congestion

#congestion_by_weather = fact_traffic_1.groupby(["weather_condition", "congestion_level"]).size().unstack(fill_value=0)

# Group by weather_condition and sum the traffic_volume
traffic_by_weather = fact_traffic_1.groupby("weather_condition")["traffic_volume"].sum().sort_values(ascending=False)

# Plot the result
traffic_by_weather.plot(kind='bar')

# Add labels and title
plt.xlabel("Weather Condition")
plt.ylabel("Total Traffic Volume")
plt.title("Total Traffic Volume by Weather Condition")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("weatherVsTraffic.png")

# Show plot
plt.show()




# Group by peak_category and sum traffic volume
volume_by_peak = fact_traffic_1.groupby("peak_category")["traffic_volume"].sum().loc[["Morning Peak", "Evening Peak"]]

# Plot the result
volume_by_peak.plot(kind='bar', color='skyblue')

# Add labels and title
plt.xlabel("Peak Category")
plt.ylabel("Total Traffic Volume")
plt.title("Total Traffic Volume: Morning vs Evening Peak")
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig("peakCategoryVsTrafficVolume.png")

# Show plot
plt.show()





# # Group by hour and calculate average traffic volume
# avg_volume_by_hour = fact_traffic_1.groupby("hour")["traffic_volume"].mean()
#
# # Plot the result
# avg_volume_by_hour.plot(kind='bar', color='coral')
#
# # Add labels and title
# plt.xlabel("Hour of the Day")
# plt.ylabel("Average Traffic Volume")
# plt.title("Average Traffic Volume by Hour")
# plt.xticks(rotation=0)
# plt.tight_layout()
#
# # Show plot
# plt.show()



# Group by congestion level and sum traffic volume
volume_by_congestion = fact_traffic_1.groupby("congestion_level")["traffic_volume"].sum()

# Plotting
volume_by_congestion.plot(kind="bar", color="teal")

# Add labels and title
plt.xlabel("Congestion Level")
plt.ylabel("Total Traffic Volume")
plt.title("Total Traffic Volume by Congestion Level")
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig("congestionVsSumTrafficVolume.png")
# Show plot
plt.show()








# # Example: Removing outliers from numeric fields (volume, speed, density)
# def remove_outliers(df, col_name):
#     q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
#     iqr = q3 - q1
#     lower_bound = q1 - 1.5 * iqr
#     upper_bound = q3 + 1.5 * iqr
#     return df.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound))
#
# numeric_cols = ['traffic_volume', 'traffic_speed', 'traffic_density', 'travel_time',
#                 'delay_reduction', 'queue_length_reduction', 'emission_levels']
#
# for col_name in numeric_cols:
#     df_clean = remove_outliers(df_clean, col_name)
#
#
# print(df_clean.value_counts())



# df_mysql = spark.read.format("jdbc").options(
#     url="jdbc:mysql://localhost:3306/traffic_analytics",
#     driver="com.mysql.cj.jdbc.Driver",
#     dbtable="fact_traffic_1",
#     user="root",
#     password="Bigdata123"
# ).load()



#
# df_mysql = df_mysql.dropna()
#
#
# df_mysql.show(5)





#
# # 2. Define expected schema for Kafka JSON messages
# expected_schema = StructType() \
#     .add("Timestamp", StringType()) \
#     .add("timestamp", StringType()) \
#     .add("Traffic_Volume", DoubleType()) \
#     .add("Traffic_Speed", DoubleType()) \
#     .add("Traffic_Density", DoubleType()) \
#     .add("Time_of_Day", DoubleType()) \
#
# # 3. Read Kafka Stream
# kafka_df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "traffic_topic") \
#     .option("startingOffsets", "earliest") \
#     .option("maxOffsetsPerTrigger", "100") \
#     .load()
#
# # 4. Convert binary Kafka value to string, then parse JSON
# parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
#     .withColumn("data", from_json(col("json_str"), expected_schema))
#
# # 5. Split valid and malformed rows
# valid_df = parsed_df.filter(col("data").isNotNull()).select("data.*")
# invalid_df = parsed_df.filter(col("data").isNull()).select("json_str")
#
# # 6. Fill missing values and normalize speed to km/h
# valid_df = valid_df.na.fill({
#     "Traffic_Speed": 0.0,
#     "speed_unit": "kmh",
#     "direction": "unknown"
# }).withColumn(
#     "speed_kmh",
#     when(col("speed_unit") == "mps", col("speed") * 3.6).otherwise(col("speed"))
# ).withColumn(
#     "timestamp", to_timestamp("timestamp")
# )
#
# # 7. Output valid rows to console
# valid_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
#
#
# #congestion traffic
# alerts_df = valid_df.filter(col("speed") < 5.0).withColumn("alert_type", lit("Congestion: Low Speed"))
#
#
# alerts_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
#
#
#
# # 8. Output malformed rows to console
# invalid_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
#
# # 9. Wait for termination
spark.streams.awaitAnyTermination()
