import pika
import time
import logging
import json
import awkward as ak
import vector

MeV = 0.001
GeV = 1.0

consumers = 3

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']

# Cut lepton type (electron type is 11,  muon type is 13)
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

# Cut lepton charge
def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV # .M calculates the invariant mass
    return invariant_mass


def process_sample(data, step_size = 1000000):
    # Define empty list to hold all data for this sample
    sample_data = []
    # Perform the cuts for each data entry in the tree
    # We can use data[~boolean] to remove entries from the data set
    
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]

    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

    # Append data to the whole sample data list
    sample_data.append(data)

    return ak.concatenate(sample_data)



def callback(ch, method, properties, body):
    incoming = ak.from_json(body)
    logging.info("recieved")
    data = process_sample(incoming)
    # Acknowledge the message
    #ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.basic_publish(exchange='', routing_key='result_queue',body=ak.to_json(data))
    logging.info("data sent")

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
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)


# Set up the consumer to consume messages from the queue
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
