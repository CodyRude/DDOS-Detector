#!/usr/bin/python3

import pyspark.sql
import pyspark.sql.functions
import pyspark.sql.types
import time
import datetime

KAFKA_ADDRESS = "localhost:9092"
KAFKA_TOPIC = "Logs"
FILENAME = "apache-access-log.txt"

# Create spark session
sparksession = pyspark.sql.SparkSession.builder.appName("LogAggregator").getOrCreate()

# Setup Schema
data_type = pyspark.sql.types.StructField("value", pyspark.sql.types.StringType())
schema = pyspark.sql.types.StructType([data_type])

# Read in data
log_df = sparksession.read.csv(FILENAME, schema=schema)
log_df = log_df.withColumn("key", pyspark.sql.functions.monotonically_increasing_id().cast("string"))

# Parse timestamps. This is done so that the logs look like they are occuring in real time
# If all the logs are dumped into kafka at once, there will be no output in the ddos detector
# until more data is given. Since we only have limited data, it must not be put in all at once.
string_time_col = pyspark.sql.functions.regexp_extract(log_df['value'], '\[(.*)\]', 1).alias("TimestampString")
log_df = log_df.withColumn("Timestamp", pyspark.sql.functions.to_timestamp(string_time_col, "dd/MMM/yyyy:HH:mm:ss Z"))

# Determine start and end time of the input data
start_time = log_df.select(pyspark.sql.functions.min(log_df['Timestamp']).alias("min")).collect()[0]['min']
end_time = log_df.select(pyspark.sql.functions.max(log_df['Timestamp']).alias("max")).collect()[0]['max']
num_time_steps = (end_time - start_time).seconds + 1

# Here is the loop where we send the logs to Kafka. The first
# iteration of the loop puts in all the logs with the starting
# timestep.  The next iteration puts in data for the next second of
# data. The timer is checked to ensure data is not entered too fast.
system_start_time = time.perf_counter()
for i in range(num_time_steps):
    current_timestamp = start_time + datetime.timedelta(seconds=i)
    
    # Ensure logs aren't being processed faster than real time:
    while((system_start_time + i) > time.perf_counter()):
        time.sleep(1)
        
    ds = log_df.where(log_df['Timestamp'] == current_timestamp) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_ADDRESS) \
    .option("topic", KAFKA_TOPIC) \
    .save()
