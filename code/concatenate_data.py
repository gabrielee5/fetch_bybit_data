import os
import pandas as pd
import glob

## concatenates the closing prices of all the assets in a specified folder

def process_price_data(folder_path, output_file):
    # Dictionary to store DataFrames for each asset
    all_data = {}
    
    # Get all CSV files in the specified folder
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process each CSV file
    for file_path in csv_files:
        # Extract asset name from filename (assuming format ASSETNAME_D_data.csv)
        filename = os.path.basename(file_path)
        asset_name = filename.split('_')[0]
        
        print(f"Processing {asset_name} from {filename}")
        
        # Read the CSV file
        try:
            df = pd.read_csv(file_path)
            
            # Assuming timestamp column exists and closing price column exists
            # You may need to adjust column names based on your actual data
            if 'timestamp' not in df.columns:
                print(f"Warning: No timestamp column in {filename}, looking for alternative")
                time_columns = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
                if time_columns:
                    df.rename(columns={time_columns[0]: 'timestamp'}, inplace=True)
                else:
                    print(f"Error: Could not find timestamp column in {filename}")
                    continue
            
            # Look for closing price column
            if 'close' in df.columns:
                close_col = 'close'
            elif 'Close' in df.columns:
                close_col = 'Close'
            else:
                # Try to find any column that might be the closing price
                close_candidates = [col for col in df.columns if 'clos' in col.lower()]
                if close_candidates:
                    close_col = close_candidates[0]
                else:
                    print(f"Error: Could not find closing price column in {filename}")
                    continue
            
            # Select only the necessary columns
            df_selected = df[['timestamp', close_col]].copy()
            
            # Rename the closing price column to the asset name
            df_selected.rename(columns={close_col: asset_name}, inplace=True)
    
            # Set timestamp as index for easier merging later
            df_selected.set_index('timestamp', inplace=True)
            
            # Store in our dictionary
            all_data[asset_name] = df_selected
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    if not all_data:
        print("No data was processed successfully")
        return
    
    # Combine all DataFrames
    print("Combining all asset data...")
    combined_df = pd.concat(all_data.values(), axis=1)
    
    # Reset index to make timestamp a column again
    combined_df.reset_index(inplace=True)
    
    # Fill NaN values (in case some assets don't have data for all timestamps)
    combined_df.fillna('', inplace=True)
    
    # Save to output file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined data saved to {output_file}")
    print(f"Total rows: {len(combined_df)}, Total assets: {len(all_data)}")

# Usage example
if __name__ == "__main__":
    # Specify the folder containing the CSV files
    data_folder = "data/20-24_data/"
    # Specify the output file
    output_file = "handle-data/20-24_D_close_prices.csv"
    
    process_price_data(data_folder, output_file)