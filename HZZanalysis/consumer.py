import infofile
import pika
import time
import logging
import json
import awkward as ak
import vector
import sys
import os

MeV = 0.001
GeV = 1.0

debug = os.getenv('DEBUG', 'False').lower() == 'true'
if debug:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
else:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])

datasets = {'A':0,'B':1,'C':2,'D':3}
lumis = [0.5,1.9,2.9,4.7]
dataset = datasets[os.getenv('DATASET', 'A').upper()]
lumi = lumis[dataset] # selects lumi based on which dataset is in use

variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

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

def calc_weight(weight_variables, sample, events):
    info = infofile.infos[sample]
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    total_weight = xsec_weight 
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

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

def mc_process_sample(data, value, step_size = 1000000):
    sample_data = []

    
        # Cuts
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]
        
        # Invariant Mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

        # Store Monte Carlo weights in the data
    data['totalWeight'] = calc_weight(weight_variables, value, data)

        # Append data to the whole sample data list
    sample_data.append(data)

    # turns sample_data back into an awkward array
    return ak.concatenate(sample_data)

def callback(ch, method, properties, body):
    incoming = ak.from_json(body)
    logging.info("recieved")
    data = process_sample(incoming)

    channel.basic_publish(exchange='', routing_key='result_queue',body=ak.to_json(data))
    logging.info("data sent")

def mc_callback(ch, method, properties, body):
    incoming = ak.from_json(body)
    logging.info("mc recieved")
    value = samples["Background $Z,t\\bar{t}$"]["list"][dataset]
    info = infofile.infos[value] # open infofile
    xsec_weight = (lumi*1000*info["xsec"])/(info["red_eff"]*info["sumw"]) #*1000 to go from fb-1 to pb-1
    data = mc_process_sample(incoming, value)
    channel.basic_publish(exchange='', routing_key='mc_result_queue',body=ak.to_json(data))
    logging.info("mc data sent")

def callback_shutdown(ch, method, properties, body):
    incoming = ak.from_json(body)
    logging.info("recieved shutdown command")
    ch.stop_consuming()
    connection.close()

def connect_to_rabbitmq():
    while True:
        try:
            logging.info("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.info("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(10)
    
# Wait for a successful connection
connection = connect_to_rabbitmq()

# Create a channel
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

# Declare a queue
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)
channel.queue_declare(queue='shutdown_queue', durable=True)
channel.queue_declare(queue='mc_task_queue', durable=True)
channel.queue_declare(queue='mc_result_queue', durable=True)


# Set up the consumer to consume messages from the queue
channel.basic_consume(queue='shutdown_queue', on_message_callback=callback_shutdown, auto_ack=True)

channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=True)
channel.basic_consume(queue='mc_task_queue', on_message_callback=mc_callback, auto_ack=True)

logging.info(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
