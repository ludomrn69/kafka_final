from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, udf, expr, to_json, struct, lit
from pyspark.sql.types import StructType, StringType, TimestampType
import io
import json
from fastavro import reader, parse_schema

# Create Spark session
spark = SparkSession.builder \
    .appName("FloodDetector") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load Avro schema from msg.avsc
with open("msg.avsc", "r") as f:
    avro_schema = parse_schema(json.load(f))

# Decode functions
def decode_avro_nick(avro_bytes):
    try:
        buffer = io.BytesIO(avro_bytes)
        records = list(reader(buffer, avro_schema))
        return records[0]["nick"]
    except Exception:
        return None

def decode_avro_msg(avro_bytes):
    try:
        buffer = io.BytesIO(avro_bytes)
        records = list(reader(buffer, avro_schema))
        return records[0]["msg"]
    except Exception:
        return None

decode_nick_udf = udf(decode_avro_nick, StringType())
decode_msg_udf = udf(decode_avro_msg, StringType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chat_all") \
    .option("startingOffsets", "earliest") \
    .load()

# Decode and create key
df_parsed = df_raw \
    .withColumn("nick", decode_nick_udf(col("value"))) \
    .withColumn("msg", decode_msg_udf(col("value"))) \
    .withColumn("key", expr("concat(nick, ':', msg)")) \
    .withColumn("timestamp", col("timestamp")) \
    .select("nick", "msg", "key", "timestamp")

# Count identical messages in 60s window
windowed_counts = df_parsed \
    .groupBy(window(col("timestamp"), "60 seconds"), col("key"), col("nick"), col("msg")) \
    .count() \
    .filter(col("count") >= 10)

# Create ban messages
ban_messages = windowed_counts.select(
    to_json(struct(
        col("nick"),
        lit("Spam detected: repeated identical messages").alias("reason")
    )).alias("value")
)

# Write ban messages to Kafka
ban_query = ban_messages.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "chat_bans") \
    .option("checkpointLocation", "/tmp/chat_bans_checkpoint") \
    .outputMode("update") \
    .start()

# Debug output (optional)
debug_query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

ban_query.awaitTermination()
debug_query.awaitTermination()