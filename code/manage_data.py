import pandas as pd
import numpy as np
import scipy
import pandas_ta as ta
import matplotlib.pyplot as plt
import time
import os
import glob

def concat_data(directory_path="/Users/gabrielefabietti/projects/fetch_data/data/", specific_path="BTCUSDT_perp_1h", save_file=True):
    # List all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, specific_path + "_*.csv"))

    # Read each file and store the DataFrames in a list
    data_frames = [pd.read_csv(file) for file in csv_files]

    # Concatenate all the DataFrames
    data = pd.concat(data_frames)

    # Remove duplicate timestamps
    data.drop_duplicates(subset='timestamp', inplace=True)

    # Convert 'timestamp' to datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Sort by timestamp
    data = data.sort_values(by='timestamp').reset_index(drop=True)

    # Set timestamp as index
    data = data.set_index('timestamp')
    
    if save_file:
        # Create a filename for the concatenated data
        output_filename = f"{specific_path}_concatenated.csv"
        output_path = os.path.join(directory_path, output_filename)
        
        # Save the concatenated data to a CSV file
        data.to_csv(output_path)
        print(f"Concatenated data saved to: {output_path}")
    
    return data

if __name__ ==  '__main__':
    spec_path = 'ETHUSDT_perp_5m'
    data = concat_data(specific_path=spec_path)