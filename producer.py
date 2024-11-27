import pika
import infofile # local file containing cross-sections, sums of weights, dataset IDs
import numpy as np # for numerical calculations such as histogramming
import matplotlib.pyplot as plt # for plotting
from matplotlib.ticker import AutoMinorLocator # for minor ticks
import uproot # for reading .root files
import awkward as ak # to represent nested data in columnar format
import vector # for 4-momentum calculations
import time
import gzip
import math
import json

start_time = time.time()

MeV = 0.001
GeV = 1.0

path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'], # data is from 2016, first four periods of data taking (ABCD)
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
    },

}

def get_tree(sample_name):
    file_path = path + "Data/" + sample_name + ".4lep.root"
    return  uproot.open(file_path + ":mini;1")

def tree_chunks(tree, chunk_size):
    for chunk in tree.iterate(library="ak", step_size=chunk_size):
        yield chunk

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
channel.queue_declare(queue='chunks_queue', durable=True)
channel.queue_declare(queue='time_queue', durable=True)
# Send numbers 1 to 100 to the queue

tree = get_tree(samples['data']['list'][0])
num_entries = tree.num_entries
chunk_size = math.ceil(num_entries/3)


chunks = 0
for chunk in tree_chunks(tree, chunk_size):
    chunks += 1
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=ak.to_json(chunk),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )
    print(f" [x] Sent")

channel.basic_publish(exchange='',routing_key='chunks_queue',body=json.dumps(chunks))
channel.basic_publish(exchange='',routing_key='time_queue',body=json.dumps(start_time))
# Close the connection
connection.close()
