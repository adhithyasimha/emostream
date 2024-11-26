import requests
import random
import time
import threading
import uuid
from kafka import KafkaConsumer
import json

class EmojiClient:
    def __init__(self):
        self.emojis = ["🏏", "🇮🇳", "🇵🇰", "🏆", "🥇", "🏟️"]
        self.base_url = "http://localhost:5000"
        self.emoji_rate = 50  # Emojis per second
        self.sleep_time = 2.0 / self.emoji_rate
       
        self.client_id = self.generate_unique_id()
        response = requests.post(
            f"{self.base_url}/connect",
            json={"client_id": self.client_id}
        )
        print(f"Connected with ID: {self.client_id}")
       
        # Kafka Consumer for 'processed_stream' topic
        self.consumer = KafkaConsumer(
            'final',  # Kafka topic
            bootstrap_servers=['localhost:9092'],  # Kafka server
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',  # Start consuming from the latest message
            enable_auto_commit=True
        )
       
    def generate_unique_id(self):
        return str(uuid.uuid4())[:6]

    def send_emoji(self):
        while True:
            # Generate and send emoji messages
            message_id = self.generate_unique_id()  # Generate new ID for each message
            emoji_data = {
                "user_id": message_id,
                "emoji_type": random.choice(self.emojis),
                "timestamp": time.time()
            }
           
            try:
                response = requests.post(
                    f"{self.base_url}/emoji",
                    json=emoji_data,
                    timeout=0.1
                )
                #print(f"{message_id} → {emoji_data['emoji_type']}")  # Send emoji
            except Exception as e:
                #print(f"Error sending emoji: {str(e)}")
                error = 1
           
            time.sleep(self.sleep_time)

    def consume_processed_stream(self):
        for message in self.consumer:
            print(f"Consumed from Kafka: {message.value}")
            # Optionally, you can process the consumed message here
            # For example, you can modify the message and send it back to a different Kafka topic or API

    def disconnect(self):
        try:
            requests.post(
                f"{self.base_url}/disconnect",
                json={"client_id": self.client_id},
                timeout=0.1
            )
            print(f"\nDisconnected: {self.client_id}")
        except Exception as e:
            print(f"Error disconnecting: {str(e)}")

def main():
    client = EmojiClient()

    # Start the thread for sending emojis
    emoji_thread = threading.Thread(target=client.send_emoji)
    emoji_thread.daemon = True
    emoji_thread.start()

    # Start the thread for consuming Kafka messages
    consume_thread = threading.Thread(target=client.consume_processed_stream)
    consume_thread.daemon = True
    consume_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.disconnect()
        print("Shutting down...")

if __name__ == '__main__':
    main()
