#!/usr/bin/env python
# coding: utf-8

# In[5]:


## Import required libraries
import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterDocument
import datetime
import time
import requests
import os
# Get the user's profile directory test git
user_profile = os.path.expanduser("~")
config_file_path = os.path.join(user_profile, "teleconfig.json")

print(config_file_path)

import json

config_data = {}

if os.path.exists(config_file_path):
    with open(config_file_path, "r") as config_file:
        config_data = json.load(config_file)
        
session_file = config_data.get("session_file")
api_id = config_data.get("api_id")
api_hash = config_data.get("api_hash")
chats = config_data.get("chats")
print(session_file)
print(api_hash)
#print(phone_number)
print(chats)

###********************************************
''' import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterDocument
import datetime
import time
'''

chats = ['banknifty_nifty_stock_s_option_s']
days = 0
# Calculate the date 3 days ago
# Calculate the date for today
today_date = datetime.date.today()
#today_date = datetime.date.today() - datetime.timedelta(days=1)
print(today_date)
async def fetch_messages():
    async with TelegramClient(session_file, api_id, api_hash) as client:
        data_list = []  # List to store data
        for chat in chats:
            async for message in client.iter_messages(chat, offset_date=today_date, reverse=True):
                data = {
                    "group": chat,
                    "sender": message.sender_id,
                    "text": message.text,
                    "date": message.date
                }
                data_list.append(data)

        # Create a DataFrame from the data_list
        df = pd.DataFrame(data_list)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df = df[df['text'].str.contains(r'Buy', case=False, regex=True)]
        return df

max_retries = 3  # Adjust as needed
retry_delay = 1  # Adjust as needed

while True:
    try:
        df = fetch_messages()
        df = await df  # Await the async function

        # Check if the DataFrame contains messages with the "BUY" pattern (case-insensitive)
        if df.empty:
            print("No messages found for today.")
            break  # Exit the loop if there are no messages for today

        buy_pattern_rows = df[df['text'].str.contains(r'BUY', case=False, regex=True)]
        if not buy_pattern_rows.empty:
            print("Found a message with the 'BUY' pattern:")
            print(buy_pattern_rows['text'])
            break  # Exit the loop if a message with "BUY" pattern is found

        time.sleep(retry_delay)  # Wait for a while before checking again
    except Exception as e:
        print(f"Caught an error: {e}")
        time.sleep(retry_delay)

print("Exiting the loop.")

##Print call or put buy
# Use boolean indexing to filter rows with the "Buy" pattern in the "Text" column
buy_pattern_rows = df[df['text'].str.contains(r'Buy', case=False, regex=True)]
#print(buy_pattern_rows['text'])
input_string = ' '.join(buy_pattern_rows['text'])
#print(input_string)
buy_index = input_string.find("Buy")

if buy_index != -1:
    text_after_buy = input_string[buy_index + 3:buy_index + 29]  # Print the text after "Buy" up to the next 50 characters
    print(text_after_buy)
else:
    print("Pattern 'Buy' not found.")

#####***********************************************
text_after_buy = text_after_buy.upper()
bank_buy = - 2
ce_buy = - 2
pe_buy = - 2

bank_buy = text_after_buy.find("BANK")
ce_buy = text_after_buy.find("CE")
pe_buy = text_after_buy.find("PE")
if bank_buy > -1 and ce_buy > -1:
    indexname = 'BANKNIFTY'
    cepe = 'CE'
elif bank_buy > -1 and pe_buy > -1:
    indexname = 'BANKNIFTY'
    cepe = 'PE'

###########***********************
import json

# Data to replace the existing content in the JSON file
data_to_insert = {
    "index_name": indexname,
    "ce_pe": cepe
}


# File path to the JSON file
json_file_path = 'C:/Users/raghv/tradestrike.json'

# Write the new data to the JSON file, overwriting the existing content
with open(json_file_path, 'w') as file:
    json.dump(data_to_insert, file, indent=4)

print("Script successful. Data replaced in tradestrike.json.")
print((indexname) + " "+cepe)
############**************************

## Import required libraries
import upstox_client
import urllib.parse
import pandas as pd
import requests
import os
import json
import math
# Get the user's profile directory
user_profile = os.path.expanduser("~")
config_file_path = os.path.join(user_profile, "tradestrike.json")

## read api keys etc from user's profile config file.

config_data = {}

if os.path.exists(config_file_path):
    with open(config_file_path, "r") as config_file:
        config_data = json.load(config_file)
        
ce_pe = config_data.get("ce_pe")
index_name = config_data.get("index_name")
print(ce_pe)
###########################***********

# Urls for fetching Data
url_bnf     = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
# Headers
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}

url_oc      = "https://www.nseindia.com/option-chain"
sess = requests.Session()
cookies = dict()

# Local methods
def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def get_data(url):
    set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==401):
        set_cookie()
        response = sess.get(url_bnf, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==200):
        return response.text
    return ""

response_text = get_data(url_bnf)
data = json.loads(response_text)
#bnf_data = get_data(url_bnf)
currExpiryDate = data["records"]["expiryDates"][0]
###############################
import pandas as pd

# Assuming 'data' is the dictionary containing your data
option_data = data['records']['data']
currexp = currExpiryDate  # Replace with your desired expiry date

# Create empty lists to store data
expiry_dates = []
strike_prices = []
call_last_prices = []
put_last_prices = []
call_identifier = []
put_identifier = []

for entry in option_data:
    strike_price = entry['strikePrice']
    expiry_date = entry['expiryDate']
#    identifier = entry['identifier']

    if expiry_date == currexp:
        if 'CE' in entry:
            ce_data = entry['CE']
            expiry_dates.append(expiry_date)
            strike_prices.append(strike_price)
            call_identifier.append(ce_data['identifier'])
            call_last_prices.append(ce_data['lastPrice'])

        if 'PE' in entry:
            pe_data = entry['PE']
            put_identifier.append(pe_data['identifier'])
            put_last_prices.append(pe_data['lastPrice'])

# Create a DataFrame
data_dict = {
    'Expiry': expiry_dates,
    'Strike Price': strike_prices,
    'Call Last Price': call_last_prices,
    'Put Last Price': put_last_prices,
    'Call Identifier': call_identifier,
    'Put Identifier': put_identifier
}

df = pd.DataFrame(data_dict)

# Display the DataFrame
#print(df)
########################################################
# Filter rows where Call Last Price is greater than 200
ce_rows = df[df['Call Last Price'] > 200]

# Sort the filtered rows in ascending order by 'Call Last Price'
ce_rows_sorted = ce_rows.sort_values(by='Call Last Price')

# Filter rows where Put Last Price is greater than 200
pe_rows = df[df['Put Last Price'] > 200]

# Sort the filtered rows in ascending order by 'Put Last Price'
pe_rows_sorted = pe_rows.sort_values(by='Put Last Price')
#print(pe_rows_sorted)
# Display the sorted rows
#print(pe_rows_sorted)
# Display the sorted rows
#print(ce_rows_sorted)

# Assuming you have sorted PE and CE rows in pe_rows_sorted and ce_rows_sorted

# Get the first value of strike price for Put (PE) and identifier
pe_strike = pe_rows_sorted.iloc[0]['Strike Price']
pe_identifier = pe_rows_sorted.iloc[0]['Put Identifier']
pe_expiry_date = pe_rows_sorted.iloc[0]['Expiry']
#print(pe_identifier)
# Get the first value of strike price for Call (CE)
ce_strike = ce_rows_sorted.iloc[0]['Strike Price']
ce_identifier = ce_rows_sorted.iloc[0]['Call Identifier']
ce_expiry_date = ce_rows_sorted.iloc[0]['Expiry']
#print(ce_identifier)

if ce_pe == "CE" and index_name == "BANKNIFTY":
    finalidentifier = ce_identifier
    final_strike = ce_strike
    final_expiry = pe_expiry_date
elif ce_pe == "PE" and index_name == "BANKNIFTY":
    finalidentifier = pe_identifier
    final_strike = pe_strike
    final_expiry = ce_expiry_date
# Display the strike prices
#print(f"Put Strike Price: {call_identifier}")
#print(f"Call Strike Price: {put_identifier}")
print(finalidentifier)
print(final_strike)
print(final_expiry)
###################################################################
fileUrl ='https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
symboldf = pd.read_csv(fileUrl)
symboldf = symboldf.dropna(subset=['tradingsymbol'])
#print(symboldf)
symboldf['expiry'] = pd.to_datetime(symboldf['expiry'])
filtered_df = symboldf[
    (symboldf['tradingsymbol'].str.contains('BANKNIFTY')) &
    (symboldf['instrument_type'] == 'OPTIDX') &
    (symboldf['expiry'] == final_expiry ) &
    (symboldf['option_type'] == ce_pe ) &
    (symboldf['strike'] == final_strike )  ## the last price is not correct we need to find some other way-
]

print(filtered_df)
instrument_key_values = max(filtered_df['instrument_key'])
print(instrument_key_values)
#####################################################################
import redis
from datetime import datetime
date_id = datetime.today().strftime('%Y%m%d%H%M%S')

r = redis.Redis(
  host='redis-11767.c301.ap-south-1-1.ec2.cloud.redislabs.com',
  port=11767,
  password='w8Q77DVwGe9a6IVKBvJi4ZWXXlVFpbdX')

# Data to insert
data = {
    "instrument_token": instrument_key_values,
    "date_id": date_id
}

# Set the data in Redis
for key, value in data.items():
    r.hset('my_hash', key, value)
#########################################################################
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# Email configuration
sender_email = "disciplinentrade@gmail.com"
receiver_email = "ssatendra01@gmail.com"
password = "myplhrybynzdbypr"
subject = "Testing-Instrument id: " + instrument_key_values
body = "Hi Testing Instrument key"

# Create the email message
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject

# Attach the email body
msg.attach(MIMEText(body, 'plain'))

# Optional: Attach a file
# with open('path_to_file.pdf', 'rb') as file:
#     part = MIMEApplication(file.read(), Name='file.pdf')
#     part['Content-Disposition'] = f'attachment; filename="file.pdf"'
#     msg.attach(part)

# Establish an SMTP connection
try:
    server = smtplib.SMTP('smtp.gmail.com', 587)  # Use the SMTP server of your email provider
    server.starttls()  # Upgrade the connection to a secure, encrypted connection
    server.login(sender_email, password)  # Login to your email account

    # Send the email
    server.sendmail(sender_email, receiver_email, msg.as_string())

    # Close the connection
    server.quit()

    print("Email sent successfully")
except Exception as e:
    print(f"Email could not be sent. Error: {str(e)}")

##################################################

