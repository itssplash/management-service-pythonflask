from flask import Flask, jsonify
import pika
import os
import json
from threading import Thread

app = Flask(__name__)

orders = []  # Store orders fetched from RabbitMQ

# Get RabbitMQ connection string from environment variable or default to localhost
RABBITMQ_CONNECTION_STRING = os.getenv('RABBITMQ_CONNECTION_STRING', 'amqp://localhost')
queue = 'order_queue'  # Queue name for orders

def consume_orders():
    """Connects to RabbitMQ and consumes messages from the queue."""
    try:
        # Parse the RabbitMQ connection string
        params = pika.URLParameters(RABBITMQ_CONNECTION_STRING)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare queue (in case it doesn't exist)
        channel.queue_declare(queue=queue, durable=False)

        # Callback function to process messages
        def callback(ch, method, properties, body):
            order = json.loads(body)  # Parse the JSON message
            orders.append(order)  # Store order in the orders list
            print(f"Received order: {order}")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message

        # Start consuming messages from the queue
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
        print("Started consuming orders from RabbitMQ...")
        channel.start_consuming()

    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")

# Expose an API endpoint to retrieve the orders
@app.route('/orders', methods=['GET'])
def get_orders():
    return jsonify(orders)  # Return the stored orders as JSON

if __name__ == '__main__':
    # Start consuming RabbitMQ messages in a background thread
    consume_thread = Thread(target=consume_orders)
    consume_thread.start()

    # Start Flask app
    port = int(os.getenv("PORT", 5000))  # Default to port 5000
    app.run(host='0.0.0.0', port=port)
