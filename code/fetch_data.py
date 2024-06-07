from pybit.unified_trading import HTTP
import pandas as pd
import time
from datetime import datetime, timedelta
from dotenv import dotenv_values
import logging
import os

# FETCH BYBIT DATA FOR A SPECIFIC PAIR AND SAVE IN DATA FOLDER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
secrets = dotenv_values(".env")
api_key = secrets["api_key"]
api_secret = secrets["api_secret"]

# Initialize the session
session = HTTP(
    api_key=api_key, 
    api_secret=api_secret
)

def fetch_bybit_data(session, symbol, interval, start_time, end_time, delay=1):
    data = []
    
    current_time = start_time
    total_requests = 0
    total_data_points = 0

    while current_time < end_time:
        next_time = current_time + 86400000  # Increment by one day (milliseconds)
        if next_time > end_time:
            next_time = end_time
        
        try:
            response = session.get_kline(
                category="linear",
                symbol=symbol,
                interval=interval,
                start=current_time,
                end=next_time
            )
            response_data = response["result"]["list"]
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            time.sleep(delay)
            continue
        
        if not response_data:
            logging.info("No more data to fetch.")
            break
        
        # Extend the data list with new response data
        data.extend(response_data)
        total_data_points += len(response_data)
        current_time = next_time
        total_requests += 1

        logging.info(f"Fetched {len(response_data)} data points. Total data points so far: {total_data_points}.")
        logging.info(f"Current progress: {(current_time - start_time) / (end_time - start_time) * 100:.2f}%")
        time.sleep(delay)  # Delay to avoid rate limits
    
    logging.info(f"Total requests made: {total_requests}")
    logging.info(f"Total data points fetched: {total_data_points}")

    # Convert the data into a DataFrame
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])

    # Remove duplicate timestamps
    df.drop_duplicates(subset='timestamp', inplace=True)
    
    return df

def save_with_versioning(dataframe, folder, base_filename):
    version = 1
    file_path = os.path.join(folder, f"{base_filename}.csv")
    
    # Check if the file already exists and add versioning
    while os.path.exists(file_path):
        file_path = os.path.join(folder, f"{base_filename}_v{version}.csv")
        version += 1
    
    dataframe.to_csv(file_path, index=False)
    return file_path


def main(symbol, start_date, end_date, interval=60):

    start_time = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")) * 1000)
    end_time = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")) * 1000)
    # end_time = start_time + months * 30 * 86400000  # n months later in milliseconds

    logging.info(f"Fetching data for {symbol} from {start_time} to {end_time} in {interval}-minute intervals.")

    # Fetch data
    data = fetch_bybit_data(session, symbol, interval, start_time, end_time)

    # Check if data is not empty
    if not data.empty:
        # Convert the timestamp to a readable date format
        data['timestamp'] = pd.to_numeric(data['timestamp'], errors='coerce') # new line to test
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        
        # Sort the data in chronological order
        data = data.sort_values(by='timestamp').reset_index(drop=True)
        
        # Save to CSV in a separate folder with versioning
        output_folder = "data"
        os.makedirs(output_folder, exist_ok=True)  # Create the folder if it doesn't exist
        output_file = save_with_versioning(data, output_folder, f"{symbol}_perp_1h_{start_date}_to_{end_date}") # change to the correct timeframe if needed
        
        logging.info(f"Data saved to {output_file}")
    else:
        logging.info("No data fetched.")


# Parameters
symbol = "SOLUSDT"
interval = 60  # 1-hour interval in minutes
# end_time = int(time.time() * 1000)  # Current time in milliseconds
# start_time = end_time - months * 30 * 86400000  # 6 months ago in milliseconds
start_date = '2024-01-01'
end_date = '2024-05-15'

if __name__ ==  '__main__':

    main(symbol, start_date, end_date, interval)
