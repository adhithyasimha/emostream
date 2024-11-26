from kafka import KafkaConsumer, KafkaProducer
import json

bootstrap_servers = 'localhost:9092'  
input_topic = 'processed_stream'
output_topic = 'final'

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=bootstrap_servers,
    group_id='consumer-group-1',  
    auto_offset_reset='earliest',  
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

print(f"Started consuming from topic '{input_topic}' and sending to '{output_topic}'...")

for message in consumer:
    try:
        incoming_message = message.value
        #print(f"Received message: {incoming_message}")


        processed_message = incoming_message  
        producer.send(output_topic, value=processed_message)
        #print(f"Sent processed message to '{output_topic}'.")

    except Exception as e:
        print(f"Error processing message: {e}")

consumer.close()
producer.close()
