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
import sys
import logging
import os

start_time = time.time()

consumers = int(os.getenv('NUM_CONSUMERS', 1))
debug = os.getenv('DEBUG', 'False').lower() == 'true'
if debug:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
else:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])

datasets = {'A':0,'B':1,'C':2,'D':3}
lumis = [0.5,1.9,2.9,4.7]
dataset = datasets[os.getenv('DATASET', 'A').upper()]
lumi = lumis[dataset] # selects lumi based on which dataset is in use

MeV = 0.001
GeV = 1.0

path = sys.argv[1]

#with open('datahref.txt', 'r') as file:
#    path = file.read()
#path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

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

weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

def get_tree(sample_name):
    file_path = path + "Data/" + sample_name + ".4lep.root"
    return  uproot.open(file_path + ":mini;1")

def tree_chunks(tree, chunk_size):
    for chunk in tree.iterate(library="ak", step_size=chunk_size):
        yield chunk

def connect_to_rabbitmq():
    while True:
        try:
            logging.info("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.info("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(5)

def get_MC_tree(mc_name):
    background_Zee_path = path + "MC/mc_"+str(infofile.infos[mc_name]["DSID"])+"."+mc_name+".4lep.root"
    return uproot.open(background_Zee_path + ":mini;1")

def send_chunks(tree, destination):
    num_entries = tree.num_entries
    chunk_size = math.ceil(num_entries/consumers)

    chunks = 0
    for chunk in tree_chunks(tree, chunk_size):
        chunks += 1
        channel.basic_publish(
            exchange='',
            routing_key=destination,
            body=ak.to_json(chunk),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )
        logging.info(f" [x] Sent {chunks}")
    return chunks
# Wait for a successful connection
connection = connect_to_rabbitmq()

# Create a channel
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='chunks_queue', durable=True)
channel.queue_declare(queue='time_queue', durable=True)
channel.queue_declare(queue='mc_task_queue', durable=True)
channel.queue_declare(queue='mc_chunks_queue', durable=True)
# Send numbers 1 to 100 to the queue

tree = get_tree(samples['data']['list'][dataset])
chunks = send_chunks(tree, 'task_queue')

MC_tree = get_MC_tree(samples["Background $Z,t\\bar{t}$"]["list"][dataset])
mc_chunks = send_chunks(MC_tree, 'mc_task_queue')


channel.basic_publish(exchange='',routing_key='chunks_queue',body=json.dumps(chunks))
channel.basic_publish(exchange='',routing_key='mc_chunks_queue',body=json.dumps(mc_chunks))
channel.basic_publish(exchange='',routing_key='time_queue',body=json.dumps(start_time))
# Close the connection
connection.close()
