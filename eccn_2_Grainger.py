import json
import requests
import xml.etree.ElementTree as ET
import pandas as pd

# Define the path to your JSON file
json_file_path = 'credentials.json'

# Open and load the JSON file
with open(json_file_path) as f:
    credentials = json.load(f)
zoho_params = credentials.get('zoho_params')

def fetch_data_from_zoho():
    # Fetch Zoho OAuth token
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "client_id": zoho_params.get("client_id"),
        "client_secret": zoho_params.get("client_secret"),
        "grant_type": zoho_params.get("grant_type"),
        "scope": zoho_params.get("scope"),
        "refresh_token": zoho_params.get("refresh_token")  # Assuming you are using a refresh token for auth
    }

    response = requests.post(url, params=params)
    if response.status_code == 200:
        access_token = response.json().get('access_token')
        print("Zoho Token obtained successfully")
    else:
        print(f"Failed to obtain Zoho token. Status code: {response.status_code}")
        return None

    # Fetch data from Zoho Analytics
    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Report%20for%20ECCN_Grainger%20Products(T-4)?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rows_data = []

        for row in root.findall('.//row'):
            row_data = {}
            for column in row:
                column_name = column.attrib.get('name')
                row_data[column_name] = column.text
            rows_data.append(row_data)

        df = pd.DataFrame(rows_data)
        return df  # Return the DataFrame
    else:
        print(f"Failed to fetch data from Zoho. Status code: {response.status_code}")
        return None

# Call the function and assign the returned DataFrame
df = fetch_data_from_zoho()

# Optionally, display the DataFrame if not None
if df is not None:
    print(df)
else:
    print("No data fetched.")
df[['ECCN','HS_code','COO']]=''
ordered_columns = ['Lead Zoho ID','version_id','Product_id','Product Name','Version Sheet.Stage','Raptor Invoice','Date of Order Received','Shipping Country','Inco Term','Raptor SKU',
                   'Grainger SKU','Grainger/Non_Grainger','Rpt_Billing_Entity_supplier','ECCN','HS_code','COO'
            
            ]
df = df.reindex(columns=ordered_columns, fill_value=None)

print(df.info())

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Path to your JSON credentials file
json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'

# Google Sheets settings
sheet_name = 'Grainger / MCM invoice products'
worksheet_name = 'Grainger(T-3)'

# Function to authorize and create a connection to Google Sheets
def connect_to_google_sheets(json_credentials_path, sheet_name):
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Authenticate using the service account credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    
    # Authorize the client
    client = gspread.authorize(credentials)
    
    # Open the Google Sheet
    sheet = client.open(sheet_name)
    
    return sheet

# Function to create or update the worksheet and upload the DataFrame
def create_or_update_worksheet(df, sheet_name, worksheet_name, json_credentials_path):
    # Connect to Google Sheets
    sheet = connect_to_google_sheets(json_credentials_path, sheet_name)
    
    try:
        # Try to select the worksheet by name
        worksheet = sheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # If the worksheet does not exist, create a new one
        worksheet = sheet.add_worksheet(title=worksheet_name, rows=df.shape[0], cols=df.shape[1])
    
    # Clear the existing content in the worksheet (optional)
    worksheet.clear()
    
    # Handle NaN values in the DataFrame (replace NaN with an empty string or any default value)
    df = df.fillna('')  # Replaces NaN values with empty strings
    
    # Append the DataFrame data to the worksheet
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())



# Call the function to upload the DataFrame to Google Sheets
create_or_update_worksheet(df, sheet_name, worksheet_name, json_credentials_path)
