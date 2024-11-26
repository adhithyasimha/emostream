from flask import Flask, request, jsonify
import threading
from kafka_producer import enqueue_emoji  # Import the function to enqueue messages for Kafka

# Initialize Flask app
app = Flask(__name__)

@app.route('/emoji', methods=['POST'])
def handle_emoji():
    """Receive emoji data and enqueue for Kafka processing."""
    emoji_data = request.json  # Get emoji data from the request
    print(f"Received emoji data: {emoji_data}")

    # Asynchronously enqueue the data for Kafka producer
    threading.Thread(target=enqueue_emoji, args=(emoji_data,)).start()

    return jsonify({"status": "success", "data": emoji_data})

@app.route('/connect', methods=['POST'])
def client_connect():
    """Handle client connection."""
    client_id = request.json.get('client_id')
    print(f"Client {client_id} connected")
    return jsonify({"status": "connected", "client_id": client_id})

@app.route('/disconnect', methods=['POST'])
def client_disconnect():
    """Handle client disconnection."""
    client_id = request.json.get('client_id')
    print(f"Client {client_id} disconnected")
    return jsonify({"status": "disconnected", "client_id": client_id})

if __name__ == '__main__':
    app.run(port=5000, debug=True)
