from kafka import KafkaProducer
import json
import time
import threading

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Queue to hold emoji data before sending
emoji_queue = []

def enqueue_emoji(emoji_data):
    """Enqueue emoji data for Kafka."""
    emoji_queue.append(emoji_data)

def flush_to_kafka():
    """Send enqueued messages to Kafka at regular intervals."""
    while True:
        if emoji_queue:
            emoji = emoji_queue.pop(0)
            producer.send('emoji_stream', value=emoji)  # Send to 'emoji_stream' topic
            producer.flush()  # Ensure message is sent to Kafka
            print(f"Sent to Kafka: {emoji}")
        time.sleep(0.5)  # Flush every 0.5 seconds

# Start the flushing thread
flush_thread = threading.Thread(target=flush_to_kafka)
flush_thread.daemon = True  # Exit with the main program
flush_thread.start()
