import pika
import json
import awkward as ak
import os
import logging
import time
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator # for minor ticks

import subprocess

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
data_chunks = []

MeV = 0.001
GeV = 1.0

expected_chunks = 0
received = 0

# Plotting functions
def setup_plot(ax, xmin, xmax, step_size, y_max):
    """Configure plot settings."""
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(0, y_max * 1.6)
    ax.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13, x=1, horizontalalignment='right')
    ax.set_ylabel(f'Events / {step_size} GeV', y=1, horizontalalignment='right')
    ax.tick_params(which='both', direction='in', top=True, right=True)
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())

def plot_data(ax, bin_centres, data_x, data_x_errors):
    """Plot data with error bars."""
    ax.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')
    ax.legend(frameon=False)

# Callback function for determining how many chunks should be waited for before plotting graph
def callback_chunks(ch, method, properties, body):
    global expected_chunks
    expected_chunks = json.loads(body)
    logging.info(f"{expected_chunks} chunks expected")

# Callback function for timing purposes
def callback_time(ch, method, properties, body):
    start_time = json.loads(body)
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"{total_time} time elapsed")
    
# Callback function for received data
def callback(ch, method, properties, body):
    global received
    global data_chunks
    data = ak.from_json(body)
    logging.info("Processing received data chunk:")
    
    data_chunks = ak.concatenate([data_chunks,data])
    received += 1

    logging.info(str(received) + " " + str(expected_chunks))
    if received == expected_chunks:
        for i in range(received):
            channel.basic_publish(exchange='', routing_key='shutdown_queue',body=json.dumps("shutdown"))
         # Histogram settings
        xmin, xmax = 80 * GeV, 250 * GeV
        step_size = 5 * GeV
        bin_edges = np.arange(xmin, xmax + step_size, step_size)
        bin_centres = bin_edges[:-1] + step_size / 2

        # Histogram data
        data_x, _ = np.histogram(data_chunks['mass'].to_numpy(), bins=bin_edges)
        data_x_errors = np.sqrt(data_x)

        # Plot data
        fig, ax = plt.subplots()
        setup_plot(ax, xmin, xmax, step_size, np.amax(data_x))
        plot_data(ax, bin_centres, data_x, data_x_errors)
        plt.savefig('./logs/plot.png')
        plt.close()
        logging.info('plot saved as plot.png')
        logging.info("Shutting down...")
        ch.stop_consuming()
        ch.close()
        connection.close()
        logging.info("Connection closed.")
        subprocess.Popen(['/bin/sh', '/app/shutdown.sh'])
        os.system('docker-compose stop rabbitmq')

        

def connect_to_rabbitmq():
    while True:
        try:
            print("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(10)


# Establish a connection to RabbitMQ
connection = connect_to_rabbitmq()
channel = connection.channel()

# Declare the queues to consume from
channel.queue_declare(queue='result_queue', durable=True)
channel.queue_declare(queue='chunks_queue', durable=True)
channel.queue_declare(queue='time_queue', durable=True)
channel.queue_declare(queue='shutdown_queue', durable=True)


# Start consuming messages from RabbitMQ
channel.basic_consume(queue='chunks_queue', on_message_callback=callback_chunks, auto_ack=True)
channel.basic_consume(queue='result_queue', on_message_callback=callback, auto_ack=True)
channel.basic_consume(queue='time_queue', on_message_callback=callback_time, auto_ack=True)

logging.info(f"Collector is listening for messages on 'result_queue'...")
channel.start_consuming()
