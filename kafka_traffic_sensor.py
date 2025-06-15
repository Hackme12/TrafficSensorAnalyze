from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, when, from_json, lit
from pyspark.sql.types import DoubleType, StringType, StructType, TimestampType

# Write to mySQL
def write_to_mysql(batch_df, batch_id):
    count = batch_df.count()
    print(count)
    print(f" Writing batch {batch_id}, Rows: {batch_df.count()}")

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/traffic_analytics") \
        .option("dbtable", "kafka_traffic_stream") \
        .option("user", "root") \
        .option("password", "Bigdata123") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# Start Spark Session
spark = (SparkSession.builder.appName("TrafficSensorStream")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
         .config("spark.jars", "/Users/pranaya/Desktop/mysql-connector-j-9.3.0.jar")
         .getOrCreate())

#Create expected schedule for consuming message from kakfa
expected_schema = StructType() \
    .add("Timestamp", StringType()) \
    .add("Traffic_Volume", DoubleType()) \
    .add("Traffic_Speed", DoubleType()) \
    .add("Traffic_Density", DoubleType()) \
    .add("Travel_Time", DoubleType()) \
    .add("Delay_Reduction", DoubleType()) \
    .add("Emission_Levels", DoubleType()) \
    .add("Queue_Length_Reduction", DoubleType()) \

# Read Kafka Stream
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_topic") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

# Data cleanup and normalization

df_json = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = df_json \
    .select(from_json(col("json_str"), expected_schema).alias("data")) \
    .select("data.*") \
    .withColumn("Timestamp", to_timestamp("Timestamp"))


# Invalid value filtering
parsed_df = parsed_df.filter(
    (col("Traffic_Volume") >= 0) &
    (col("Traffic_Speed") >= 0) &
    (col("Traffic_Density") >= 0) &
    (col("Travel_Time") >= 0) &
    (col("Emission_Levels") >= 0)
)

# # drop duplicates
# parsed_df = parsed_df.dropDuplicates(["Timestamp", "Traffic_Volume"])

# Split valid and malformed rows
valid_df = parsed_df.filter(col("data").isNotNull())
invalid_df = parsed_df.filter(col("data").isNull())

# Fill missing values and normalize speed to km/h
valid_df = valid_df.filter(
    (col("Timestamp").isNotNull()) &
    (col("Traffic_Volume") >= 0)
)

# valid_df.show(truncate=False)
# Output valid rows to console
valid_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# print schema
valid_df.printSchema()

# Create alert for congestion traffic
alerts_df = valid_df.filter(col("Traffic_Speed") < 5.0).withColumn("alert_type", lit("Congestion: Low Speed"))

alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Output malformed rows to console
invalid_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()