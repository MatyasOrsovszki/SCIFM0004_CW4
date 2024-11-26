import pika
import time

def connect_to_rabbitmq():
    while True:
        try:
            print("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(5)

# Wait for a successful connection
connection = connect_to_rabbitmq()

# Create a channel
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='task_queue', durable=True)
# Send numbers 1 to 100 to the queue
for i in range(1, 101):
    message = str(i)
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f" [x] Sent {message}")

# Close the connection
connection.close()
