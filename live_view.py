#!/usr/bin/python3

import pyspark.sql
import pyspark.sql.functions
import pyspark.sql.types
import time
import datetime

DATA_SOURCE = "results/"


# Create spark session
sparksession = pyspark.sql.SparkSession.builder.appName("LogViewer").getOrCreate()

# Setup Schema
data_type = pyspark.sql.types.StructField("value", pyspark.sql.types.StringType())
schema = pyspark.sql.types.StructType([data_type])

# Stream input data
log_df = sparksession \
    .readStream \
    .schema(schema) \
    .text(DATA_SOURCE)

# Stream to Kafka
ds = log_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait to be shutdown
ds.awaitTermination()
