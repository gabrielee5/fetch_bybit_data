import pandas as pd
import numpy as np
import scipy
import pandas_ta as ta
import matplotlib.pyplot as plt
import time
import os
import glob

def concat_data(directory_path="/Users/gabrielefabietti/projects/fetch_data/data/", specific_path="BTCUSDT_perp_1h"):
    # use this if the data is all in one file
    # data = pd.read_csv("/Users/gabrielefabietti/projects/fetch_data/data/ETHUSDT_perp_1h_2021-06-01_to_2022-01-01.csv")

    # use this if the data is in multiple files
    # directory_path = "/Users/gabrielefabietti/projects/fetch_data/data/"

    # List all CSV files in the directory
    csv_files = glob.glob(directory_path + specific_path + "_*.csv")

    # Read each file and store the DataFrames in a list
    data_frames = [pd.read_csv(file) for file in csv_files]

    # Concatenate all the DataFrames
    data = pd.concat(data_frames)

    # Remove duplicate timestamps
    data.drop_duplicates(subset='timestamp', inplace=True)

    # Sort by timestamp
    data = data.sort_values(by='timestamp').reset_index(drop=True)

    # Convert 'timestamp' to datetime and set as index
    data['timestamp'] = data['timestamp'].astype('datetime64[s]')
    data = data.set_index('timestamp')
    
    return data
