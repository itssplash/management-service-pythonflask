import pika
import json
import logging
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
from threading import Thread

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# RabbitMQ connection details
RABBITMQ_CONNECTION_STRING = os.getenv('RABBITMQ_CONNECTION_STRING', 'amqp://localhost')
QUEUE_NAME = 'order_queue'

# Store orders fetched from RabbitMQ
orders = []

def consume_orders():
    """Connects to RabbitMQ and consumes messages from the queue."""
    try:
        # Parse the RabbitMQ connection string
        params = pika.URLParameters(RABBITMQ_CONNECTION_STRING)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare the queue (in case it doesn't exist)
        channel.queue_declare(queue=QUEUE_NAME, durable=False)

        # Callback function to process messages
        def callback(ch, method, properties, body):
            order = json.loads(body)  # Parse the JSON message
            orders.append(order)  # Store order in the orders list
            logging.info(f"Received order: {order}")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message

        # Start consuming messages from the queue
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
        logging.info("Started consuming orders from RabbitMQ...")
        channel.start_consuming()

    except Exception as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")

# Expose an API endpoint to retrieve all stored orders
@app.route('/orders', methods=['GET'])
def get_all_orders():
    return jsonify(orders), 200  # Return all stored orders

# Expose an API endpoint to retrieve the most recent order
@app.route('/orders/latest', methods=['GET'])
def get_latest_order():
    if orders:
        return jsonify(orders[-1]), 200  # Return the most recent order
    return jsonify({"message": "No orders available"}), 404

if __name__ == '__main__':
    # Start consuming RabbitMQ messages in a background thread
    consume_thread = Thread(target=consume_orders)
    consume_thread.start()

    # Start Flask app
    port = int(os.getenv("PORT", 5000))  # Default to port 5000
    app.run(host='0.0.0.0', port=port)
