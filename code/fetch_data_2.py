from pybit.unified_trading import HTTP
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import time 
import json
from dotenv import dotenv_values
import os

# Load environment variables from .env file
secrets = dotenv_values(".env")
api_key = secrets["api_key"]
api_secret = secrets["api_secret"]

# Initialize the session
session = HTTP(
    api_key=api_key, 
    api_secret=api_secret
)

def format_data(response):
    '''
    Parameters
    ----------
    respone : dict
        response from calling get_klines() method from pybit.

    Returns
    -------
    dataframe of ohlc data with date as index

    '''
    data = response.get('list', None)
    
    if not data:
        return 
    
    data = pd.DataFrame(data,
                        columns =[
                            'timestamp',
                            'open',
                            'high',
                            'low',
                            'close',
                            'volume',
                            'turnover'
                            ],
                        )
    
    f = lambda x: dt.datetime.fromtimestamp(int(x)/1000, dt.UTC)
    return data[::-1].apply(pd.to_numeric)

def get_last_timestamp(df):
    return int(df.timestamp[-1:].values[0])

def save_dataframe_to_csv(df, filename, folder_name='data_n'):
    """
    Save the given DataFrame as a CSV file in a subfolder of the main 'data' directory.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to be saved
    filename : str
        The name of the file (without .csv extension)
    folder_name : str, default='data_n'
        The name of the subfolder within the 'data' directory
    
    Returns:
    --------
    None
    """
    # Create the main 'data' folder if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Create the subfolder inside 'data' if it doesn't exist
    subfolder_path = os.path.join('data', folder_name)
    if not os.path.exists(subfolder_path):
        os.makedirs(subfolder_path)
    
    # Construct the full file path
    file_path = os.path.join('data', folder_name, f"{filename}.csv")
    
    # Save the DataFrame as CSV
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved to {file_path}")

if __name__ == '__main__':

    start = int(dt.datetime(2021, 1, 1).timestamp()* 1000)

    interval = '60' # 1,3,5,15,30,60,120,240,360,720,D,M,W
    symbol = 'SOLUSDT'
    df = pd.DataFrame()
    folder_name = 'data4'

    while True:
        response = session.get_kline(category='linear', 
                                    symbol=symbol, 
                                    start=start,
                                    interval=interval).get('result')
        
        latest = format_data(response)
        
        if not isinstance(latest, pd.DataFrame):
            break
        
        start = get_last_timestamp(latest)
        
        time.sleep(0.1)
        
        df = pd.concat([df, latest])
        print(f'Collecting data starting {dt.datetime.fromtimestamp(start/1000)}')
        if len(latest) == 1: break


    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)

    # Save the DataFrame to a CSV file
    save_dataframe_to_csv(df, f"{symbol}_{interval}_data", folder_name)