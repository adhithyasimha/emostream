
## EmoStream

## Prerequisites
- **Kafka**: Apache Kafka 3.7.1
- **Spark**: Apache Spark 3.3.0 with Kafka integration
- **Python**: Python 3.x (for running test scripts)

## Installation
1. **Download Kafka**:
   - Download Apache Kafka from [Apache Kafka Downloads](https://kafka.apache.org/downloads).
   - Extract the files and navigate to the extracted directory.
2. **Start Zookeeper**:
   ```
   bash bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
3. **Start Kafka Broker**:
   ```
   bash bin/kafka-server-start.sh config/server.properties
   ```

## Usage
1. **Create Kafka Topics**:
   - Create the `emoji_stream` topic:
     ```
     ./kafka-topics.sh --create --topic emoji_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```
   - Create the `processed_stream` topic:
     ```
     ./kafka-topics.sh --create --topic processed_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```
   - Create the `final` topic:
     ```
     ./kafka-topics.sh --create --topic final --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```
2. **Run the API**:
   ```
   python api.py
   ```
3. **Run the Client Script**:
   ```
   python client.py
   ```
4. **Run the Spark Job**:
   ```
   python3 spark.py emoji_stream
   ```
5. **Run the pubsub**:
   ```
   python3 pubsub.py
   ```


## Dependencies
- pyspark
- kafka-python

## Troubleshooting
- **Kafka Errors**: Ensure Zookeeper and Kafka brokers are running properly. Check the logs in the `logs/` directory.
- **Spark Errors**: Ensure Spark is installed and configured to communicate with Kafka. Use the correct `spark-sql-kafka` version.

