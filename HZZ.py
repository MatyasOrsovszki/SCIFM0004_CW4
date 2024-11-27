import infofile # local file containing cross-sections, sums of weights, dataset IDs
import numpy as np # for numerical calculations such as histogramming
import matplotlib.pyplot as plt # for plotting
import matplotlib_inline # to edit the inline plot format
matplotlib_inline.backend_inline.set_matplotlib_formats('pdf', 'svg') # to make plots in pdf (vector) format
from matplotlib.ticker import AutoMinorLocator # for minor ticks
import uproot # for reading .root files
import awkward as ak # to represent nested data in columnar format
import vector # for 4-momentum calculations
import time

start_time = time.time()

MeV = 0.001
GeV = 1.0

# ATLAS Open Data directory
path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

# For identification and naming
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


def process_sample(tree, variables, step_size = 1000000):
    # Define empty list to hold all data for this sample
    sample_data = []

    # Perform the cuts for each data entry in the tree
    for data in tree.iterate(variables, library="ak", step_size = step_size): # the data will be in the form of an awkward array
        # We can use data[~boolean] to remove entries from the data set
        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]

        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

        # Append data to the whole sample data list
        sample_data.append(data)

    return ak.concatenate(sample_data)

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


if __name__ == "__main__":
    variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
    tree = get_tree(samples['data']['list'][0])
    data = process_sample(tree, variables)
    tree.close()

     # Histogram settings
    xmin, xmax = 80 * GeV, 250 * GeV
    step_size = 5 * GeV
    bin_edges = np.arange(xmin, xmax + step_size, step_size)
    bin_centres = bin_edges[:-1] + step_size / 2

    # Histogram data
    data_x, _ = np.histogram(data['mass'].to_numpy(), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)

    # Plot data
    fig, ax = plt.subplots()
    setup_plot(ax, xmin, xmax, step_size, np.amax(data_x))
    plot_data(ax, bin_centres, data_x, data_x_errors)
    #plt.show()

    
print(time.time()-start_time)
