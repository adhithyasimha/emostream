#!/usr/bin/env python3
from kafka import KafkaConsumer
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import time
from collections import defaultdict
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Replace with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the message as JSON
)
spark = SparkSession.builder \
    .appName("Real-time Emoji Frequency Analysis") \
    .getOrCreate()

topic = sys.argv[1]

# Kafka consumer setup
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',  # Kafka broker address
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON messages
    auto_offset_reset='earliest',  # Start reading from the earliest message
    group_id='emoji-consumer-group'  # Consumer group id
)

emoji_counts_dict = defaultdict(int)

reference_time = None
batch_interval = 2  
first_message = True

for message in consumer:
    message_value = message.value
    message_timestamp = message_value.get('timestamp', None)
    emoji = message_value.get('emoji_type', None)

    if first_message:
        reference_time = message_timestamp
        first_message = False
        emoji_counts_dict[emoji] += 1
        continue  
    current_time = time.time()

    emoji_counts_dict[emoji] += 1

    # Process every `batch_interval` seconds
    if current_time - reference_time >= batch_interval:
        reference_time = current_time

        emoji_data = [(emoji, count) for emoji, count in emoji_counts_dict.items()]
        schema = StructType([StructField("emoji", StringType(), True), StructField("count", StringType(), True)])

        df = spark.createDataFrame(emoji_data, schema)
        sorted_emoji_counts = df.orderBy(col("count").desc())
        most_common_emoji = sorted_emoji_counts.limit(1)

        most_common_emoji_val = most_common_emoji.select("emoji").first()['emoji']
        producer.send('processed_stream', value=most_common_emoji_val)
        print(most_common_emoji_val)
        emoji_counts_dict = defaultdict(int)
        emoji_counts_dict.clear

producer.flush()
producer.close()
consumer.close()
