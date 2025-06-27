from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, udf
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

# Define decode function and register as UDF
def decode_avro(avro_bytes):
    try:
        buffer = io.BytesIO(avro_bytes)
        records = list(reader(buffer, avro_schema))
        return records[0]["nick"]
    except Exception:
        return None

decode_udf = udf(decode_avro, StringType())

# Read messages from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "chat_all") \
    .option("startingOffsets", "earliest") \
    .load()

# Use UDF to decode Avro and extract nick
df_parsed = df_raw \
    .withColumn("nick", decode_udf(col("value"))) \
    .select("nick", "timestamp")

# Count messages per user in 5-second windows
windowed_counts = df_parsed \
    .groupBy(window(col("timestamp"), "5 seconds"), col("nick")) \
    .count() \
    .filter("count > 7") \
    .select(col("nick"), col("count"))

# Output to console for debug (you can later send to Kafka)
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Ban message formatting and sending to Kafka
from pyspark.sql.functions import to_json, struct, lit

# Create a new DataFrame with nick and a ban reason message
ban_messages = windowed_counts.select(
    to_json(struct(
        col("nick"),
        lit("Flood detected: too many messages in short time").alias("reason")
    )).alias("value")
)

# Write ban messages to Kafka topic 'chat_bans'
ban_query = ban_messages.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "chat_bans") \
    .option("checkpointLocation", "/tmp/chat_bans_checkpoint") \
    .outputMode("update") \
    .start()

query.awaitTermination()
ban_query.awaitTermination()