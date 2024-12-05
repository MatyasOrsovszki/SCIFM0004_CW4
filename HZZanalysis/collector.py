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

from collections import defaultdict

debug = os.getenv('DEBUG', 'False').lower() == 'true'
if debug:
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
else:
    logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
data_chunks = defaultdict(list)
mc_data_chunks = defaultdict(list)
all_data = defaultdict(list)

MeV = 0.001
GeV = 1.0

expected_chunks = 0
expected_mc_chunks = 0
received = 0
mc_received = 0

lumi = 10.0
fraction = 1.0

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

def save_plot(mc=""):
    timestamp = time.time()
    local_time = time.localtime(timestamp)
    name = time.strftime("%d-%m-%Y %H-%M", local_time)
        
    plt.savefig(f'/app/logs/{name}{mc}.png')
    plt.close()
    logging.info(f'plot saved as {name}{mc}.png')
    
# Callback function for determining how many chunks should be waited for before plotting graph
def callback_chunks(ch, method, properties, body):
    global expected_chunks
    global all_data
    expected_chunks = json.loads(body)
    logging.info(f"{expected_chunks} chunks expected")
    while True:
        if received >= expected_chunks:
            # Histogram settings
            for identifier, chunks in data_chunks.items():
                all_data[identifier] = ak.concatenate(chunks)
             
            """xmin, xmax = 80 * GeV, 250 * GeV
            step_size = 5 * GeV
            bin_edges = np.arange(xmin, xmax + step_size, step_size)
            bin_centres = bin_edges[:-1] + step_size / 2

            # Histogram data
            data_x, _ = np.histogram(data_chunks['mass'].to_numpy(), bins=bin_edges)
            data_x_errors = np.sqrt(data_x)

            # Plot data
            fig, ax = plt.subplots()
            setup_plot(ax, xmin, xmax, step_size, np.amax(data_x))
            ax.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')
            ax.legend(frameon=False)

            save_plot()"""
            break

def callback_mc_chunks(ch, method, properties, body):
    global expected_mc_chunks
    expected_mc_chunks = json.loads(body)
    logging.info(f"{expected_mc_chunks} mc chunks expected")
    
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
    
    message = json.loads(body)
    data = ak.from_json(message["data"])
    identifier = message["identifier"]
    #data = {"data":data, "identifier":identifier}
    
    logging.info("Processing received data chunk:")

    #all_data[identifier] = ak.concatenate([all_data[identifier],data])
    data_chunks[identifier].append(data)
    received += 1

    logging.info(str(received) + " " + str(expected_chunks))
    
        

def mc_callback(ch, method, properties, body):
    global mc_received
    global mc_data_chunks
    
    message = json.loads(body)
    data = ak.from_json(message["data"])
    identifier = message["identifier"]
    
    logging.info("Processing mc data chunk:")

    #all_data[identifier] = ak.concatenate([all_data[identifier],data])
    mc_data_chunks[identifier].append(data)
    mc_received += 1

    logging.info("received: " + str(mc_received) + " expected:" + str(expected_mc_chunks))
    if mc_received >= expected_mc_chunks and expected_mc_chunks != 0:
        for identifier, chunks in mc_data_chunks.items():
                all_data[identifier] = ak.concatenate(chunks)
        for i in range(received):
            channel.basic_publish(exchange='', routing_key='shutdown_queue',body=json.dumps("shutdown"))
        # Histogram settings
        
        xmin, xmax = 80 * GeV, 250 * GeV
        step_size = 5 * GeV
        bin_edges = np.arange(xmin, xmax + step_size, step_size)
        bin_centres = bin_edges[:-1] + step_size / 2

        data_x,_ = np.histogram(ak.to_numpy(all_data['data']['mass']),bins=bin_edges ) # histogram the data
        data_x_errors = np.sqrt( data_x ) # statistical error on the data

        signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass']) # histogram the signal
        signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
        signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

        mc_x = [] # define list to hold the Monte Carlo histogram entries
        mc_weights = [] # define list to hold the Monte Carlo weights
        mc_colors = [] # define list to hold the colors of the Monte Carlo bars
        mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

        for s in samples: # loop over samples
            if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
                mc_x.append( ak.to_numpy(all_data[s]['mass']) ) # append to the list of Monte Carlo histogram entries
                mc_weights.append( ak.to_numpy(all_data[s].totalWeight) ) # append to the list of Monte Carlo weights
                mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
                mc_labels.append( s ) # append to the list of Monte Carlo legend labels

        """
        # Plot data
        fig, main_axes = plt.subplots()
        setup_plot(main_axes, xmin, xmax, step_size, np.amax(data_x))
        mc_x = ak.to_numpy(mc_data_chunks["mass"]) # define list to hold the Monte Carlo histogram entries
        mc_weights = ak.to_numpy(mc_data_chunks["totalWeight"]) # define list to hold the Monte Carlo weights
        mc_colors = samples["Background $Z,t\\bar{t}$"]['color'] # define list to hold the colors of the Monte Carlo bars
        mc_labels = "Background $Z \\to ee$" # define list to hold the legend labels of the Monte Carlo bars

        main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,fmt='ko',label='Data') 

        # plot the Monte Carlo bars
        mc_heights = main_axes.hist(mc_x, bins=bin_edges, weights=mc_weights, stacked=True, color=mc_colors, label=mc_labels)
        mc_x_tot = mc_heights[0] # stacked background MC y-axis value
        mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
        main_axes.bar(bin_centres,2*mc_x_err, alpha=0.5, bottom=mc_x_tot-mc_x_err, color='none', hatch="////", width=step_size, label='Stat. Unc.' )
        main_axes.legend( frameon=False ); # no box around the legend
        """
        # *************
        # Main plot 
        # *************
        main_axes = plt.gca() # get current axes

        # plot the data points
        main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                            fmt='ko', # 'k' means black and 'o' is for circles 
                            label='Data') 

        # plot the Monte Carlo bars
        mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                    weights=mc_weights, stacked=True, 
                                    color=mc_colors, label=mc_labels )

        mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value

        # calculate MC statistical uncertainty: sqrt(sum w^2)
        mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])

        # plot the signal bar
        signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                        weights=signal_weights, color=signal_color,
                        label=r'Signal ($m_H$ = 125 GeV)')

        # plot the statistical uncertainty
        main_axes.bar(bin_centres, # x
                        2*mc_x_err, # heights
                        alpha=0.5, # half transparency
                        bottom=mc_x_tot-mc_x_err, color='none', 
                        hatch="////", width=step_size, label='Stat. Unc.' )

        # set the x-limit of the main axes
        main_axes.set_xlim( left=xmin, right=xmax ) 

        # separation of x axis minor ticks
        main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 

        # set the axis tick parameters for the main axes
        main_axes.tick_params(which='both', # ticks on both x and y axes
                                direction='in', # Put ticks inside and outside the axes
                                top=True, # draw ticks on the top axis
                                right=True ) # draw ticks on right axis

        # x-axis label
        main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                            fontsize=13, x=1, horizontalalignment='right' )

        # write y-axis label for main axes
        main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                                y=1, horizontalalignment='right') 

        # set y-axis limits for main axes
        main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )

        # add minor ticks on y-axis for main axes
        main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

        # Add text 'ATLAS Open Data' on plot
        plt.text(0.05, # x
                    0.93, # y
                    'ATLAS Open Data', # text
                    transform=main_axes.transAxes, # coordinate system used is that of main_axes
                    fontsize=13 ) 

        # Add text 'for education' on plot
        plt.text(0.05, # x
                    0.88, # y
                    'for education', # text
                    transform=main_axes.transAxes, # coordinate system used is that of main_axes
                    style='italic',
                    fontsize=8 ) 

        # Add energy and luminosity
        lumi_used = str(lumi*fraction) # luminosity to write on the plot
        plt.text(0.05, # x
                    0.82, # y
                    '$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
                    transform=main_axes.transAxes ) # coordinate system used is that of main_axes

        # Add a label for the analysis carried out
        plt.text(0.05, # x
                    0.76, # y
                    r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
                    transform=main_axes.transAxes ) # coordinate system used is that of main_axes

        # draw the legend
        main_axes.legend( frameon=False ) # no box around the legend
        save_plot("mc")
        plt.close()
        
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
            logging.info("Attempting to connect to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.info("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(10)


# Establish a connection to RabbitMQ
connection = connect_to_rabbitmq()
channel = connection.channel()

# Declare the queues to consume from
channel.queue_declare(queue='result_queue', durable=True)
channel.queue_declare(queue='chunks_queue', durable=True)
channel.queue_declare(queue='mc_chunks_queue', durable=True)
channel.queue_declare(queue='time_queue', durable=True)
channel.queue_declare(queue='shutdown_queue', durable=True)
channel.queue_declare(queue='mc_result_queue', durable=True)


# Start consuming messages from RabbitMQ
channel.basic_consume(queue='chunks_queue', on_message_callback=callback_chunks, auto_ack=True)
channel.basic_consume(queue='mc_chunks_queue', on_message_callback=callback_mc_chunks, auto_ack=True)
channel.basic_consume(queue='result_queue', on_message_callback=callback, auto_ack=True)
channel.basic_consume(queue='mc_result_queue', on_message_callback=mc_callback, auto_ack=True)
channel.basic_consume(queue='time_queue', on_message_callback=callback_time, auto_ack=True)
logging.info(f"Collector is listening for messages on 'result_queue'...")
logging.info(f"Collector is listening for messages on 'mc_result_queue'...")
channel.start_consuming()

