#!/usr/bin/python3
import pyspark.sql
import pyspark.sql.functions


# Configuration options
# Address of Kafka server
KAFKA_ADDRESS = "localhost:9092"

# Topic where the logs are streamed
KAFKA_TOPIC = "Logs"

# Length of window used to sum data
WINDOW_LENGTH = "20 second"

# Sliding window step size
SLIDING_LEGNTH = "10 second"

# Time to wait before ignoring future data. Set to 0 to help make sure
# data outputs when using small sample data
WATERMARK_TIME = "0 second"

# Threshold for number of total requests needed to mark source as
# being part of DDOS attack. Should be adjusted according to window
# length
COUNTS_CUTOFF = 50

# Folder where results are written
RESULTS_FOLDER = "results"

# Folder where checkpoints are stored
CHECKPOINT_FOLDER = "checkpoints"


# Create spark session
sparksession = pyspark.sql.SparkSession.builder.appName("DDOS-Detector").getOrCreate()


# Create streaming dataframe from Kafka server
df = sparksession.readStream.format("kafka")
df = df.option("kafka.bootstrap.servers", KAFKA_ADDRESS)
df = df.option("subscribe", KAFKA_TOPIC)
df = df.option("startingOffsets", "earliest")
df = df.load()


# Parse timestamps from Apache logs
time_string_col = pyspark.sql.functions.regexp_extract(df['value'].cast("string"),
                                                       '\[(.*)\]', 1).alias("TimestampString")
time_col = pyspark.sql.functions.to_timestamp(time_string_col,
                                              "dd/MMM/yyyy:HH:mm:ss Z").alias("Timestamp")

# Extract IP Address from apache logs
ip_col = pyspark.sql.functions.regexp_extract(df['value'],
                                              "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",
                                              0).alias("IP")

# Create dataframe from timestamps and IP addresses
parsed_df = df.select(ip_col, time_col)

# Count number of requests within a sliding window
ip_hits_windowed = parsed_df \
    .withWatermark("Timestamp", WATERMARK_TIME). \
    groupBy("IP", pyspark.sql.functions.window("Timestamp", WINDOW_LENGTH, SLIDING_LEGNTH)) \
    .count()

# Filter data so only offenders are selected
filter_ip_hits = ip_hits_windowed.where(ip_hits_windowed['count'] >= COUNTS_CUTOFF)


# Start query stream, and write results out to a folder
query = filter_ip_hits.select("IP").writeStream \
    .format("csv") \
    .option("checkpointLocation", CHECKPOINT_FOLDER) \
    .option("path", RESULTS_FOLDER) \
    .start()

# Wait for termination
query.awaitTermination()
