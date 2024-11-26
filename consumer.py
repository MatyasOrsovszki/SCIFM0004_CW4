import pika
import time
import logging
import json

consumers = 3

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

def callback(ch, method, properties, body):
    number = json.loads(body)
    with open("output",'a') as file:
        file.write(str(number)+"\n")
    print(f" [x] Received {number}")
    logging.info(number)

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def connect_to_rabbitmq():
    while True:
        try:
            print("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(10)

# Wait for a successful connection
connection = connect_to_rabbitmq()

# Create a channel
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

# Declare a queue
channel.queue_declare(queue='ready_queue', durable=False)
channel.queue_declare(queue='task_queue', durable=True)

channel.basic_publish(exchange='', routing_key='ready_queue', body='ready')

def all_ready():
    queue_state = channel.queue_declare(queue='ready_queue',passive=True)
    print(queue_state, " consumers ready")
    return queue_state.method.message_count >= consumers

while not all_ready():
    time.sleep(5)

# Set up the consumer to consume messages from the queue
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
