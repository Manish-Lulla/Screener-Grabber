import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import os

# Function to fetch stock data from a screener
def fetch_stock_data(url, condition):
    with requests.session() as s:
        # Get the page to scrape CSRF token
        r_data = s.get(url)
        soup = bs(r_data.content, "lxml")
        
        # Get CSRF token for the POST request
        meta = soup.find("meta", {"name" : "csrf-token"})["content"]

        # Headers for the POST request
        header = {"x-csrf-token": meta}

        # Post the condition to the screener
        data = s.post(url, headers=header, data=condition).json()

        # Create a DataFrame from the returned JSON data
        stock_list = pd.DataFrame(data["data"])
        return stock_list

# Screener URLs
url = "https://chartink.com/screener/process"

# Conditions for each screener (adjust as needed)
condition_1 = {"scan_clause": "( {cash} ( latest wma( close,1 ) > monthly wma( close,2 ) + 1 and latest close > 25 and latest close <= 500 and weekly volume > 85000 ) )"}
condition_2 = {"scan_clause": "( {cash} ( latest ( (1 candle ago high + 1 candle ago low + (1 candle ago close / 3)) * 2 - 1 candle ago low ) >= 20 and latest ema( latest close , 20 ) >= 30 and latest avg true range( 14 ) >= 50 and latest avg true range( 14 ) <= 200 and latest rsi( 14 ) >= 20 and latest ( sma( sma( (close - ( ( max(7, high) + min(7, low) ) / 2 )), 7 ), 3)/(sma( sma( (max(7, high) - min(7, low) / 2 ), 7 ), 3) ) * 100 ) <= 23 ) )"}
condition_3 = {"scan_clause": "( {33489} ( weekly rsi( 14 ) > 60 and 1 week ago  rsi( 14 ) <= 60 ) )"}
condition_4 = {"scan_clause": "( {cash} ( latest rsi( 14 ) >= 65 and latest close >= 25 and latest volume > 20000 and latest close / monthly max( 60 , monthly high ) > 0.9 ) )"}
condition_5 = {"scan_clause": "( {cash} ( ( {cash} ( latest avg true range( 15 ) < 10 days ago avg true range( 15 ) and latest avg true range( 15 ) / latest close < 0.08 and latest close > ( weekly max( 52 , weekly close ) * 0.75 ) and latest ema( latest close , 50 ) > latest ema( latest close , 150 ) and latest ema( latest close , 150 ) > latest ema( latest close , 200 ) and latest close > latest ema( latest close , 50 ) and latest close > 9 and latest close * latest volume > 800000 ) ) ) )"}

# Fetch data from all screeners
stock_list_1 = fetch_stock_data(url, condition_1)
stock_list_2 = fetch_stock_data(url, condition_2)
stock_list_3 = fetch_stock_data(url, condition_3)
stock_list_4 = fetch_stock_data(url, condition_4)
stock_list_5 = fetch_stock_data(url, condition_5)

# Merge all data into a single DataFrame
merged_stock_list = pd.concat([stock_list_1, stock_list_2, stock_list_3, stock_list_4, stock_list_5], ignore_index=True)

# Optionally remove duplicates based on 'name' or any other column
merged_stock_list.drop_duplicates(subset=['name'], inplace=True)

# Define the CSV file path
csv_file = "merged_stock_list.csv"

# Check if CSV already exists
if os.path.exists(csv_file):
    # Load existing data from CSV
    existing_data = pd.read_csv(csv_file)
    # Append the new data
    merged_stock_list = pd.concat([existing_data, merged_stock_list], ignore_index=True)
    # Remove duplicates
    merged_stock_list.drop_duplicates(subset=['name'], inplace=True)

# Save the updated stock list to CSV
merged_stock_list.to_csv(csv_file, index=False)

# Display the merged stock list
print(merged_stock_list)
