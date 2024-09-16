#@author Bijoy krishan
import json
import gspread # type: ignore
import pandas as pd # type: ignore
import re
import numpy as np # type: ignore
import requests
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials # type: ignore

# Load credentials from credentials.json
with open('credentials.json') as f:
    credentials = json.load(f)

# Extract credentials
zoho_params = credentials.get('zoho_params')

# FedEx API Credentials
CLIENT_ID = fedex_params.get('CLIENT_ID')
CLIENT_SECRET = fedex_params.get('CLIENT_SECRET')

# UPS API Credentials
client_key = ups_params.get('client_key')
client_secret = ups_params.get('client_secret')

# Step 1: Fetch data from Zoho
def fetch_data_from_zoho():
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "client_id": zoho_params.get("client_id"),
        "client_secret": zoho_params.get("client_secret"),
        "grant_type": zoho_params.get("grant_type"),
        "scope": zoho_params.get("scope"),
        "soid": zoho_params.get("soid")
    }

    response = requests.post(url, params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print("Zoho Token obtained successfully")
    else:
        print(f"Failed to obtain Zoho token. Status code: {response.status_code}")
        exit()

    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Report%20for%20ECCN_Grainger%20Products?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"

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

        # Reorder columns as needed
        ordered_columns=['Subform_id','Lead Zoho ID','version_id','Product_id','Product Name','Version Sheet.Stage','Raptor Invoice','Date of Order Received','Shipping Country','Inco Term','Raptor SKU',
                   'Grainger SKU','Grainger/Non_Grainger','Rpt_Billing_Entity_supplier','ECCN','HS_code','COO'
            
            ]
        df = df.reindex(columns=ordered_columns, fill_value=None)
        print(f"Fetched {len(df)} rows from Zoho.")
        return df
    else:
        print(f"Failed to fetch data from Zoho. Status code: {response.status_code}")
        return pd.DataFrame()

# Step 2: Fetch data from Google Sheets
def fetch_data_from_gsheets(sheet_name, worksheet_name, json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    print(f"Fetched {len(df)} rows from Google Sheets.")
    return df

# Step 3: Identify missing rows in Google Sheets
def identify_missing_rows(zoho_df, gsheets_df):
    print("Zoho columns:", zoho_df.columns.tolist())
    print("Google Sheets columns:", gsheets_df.columns.tolist())

    common_columns = list(set(zoho_df.columns) & set(gsheets_df.columns))
    print("Common columns:", common_columns)

    zoho_df = zoho_df[common_columns]
    gsheets_df = gsheets_df[common_columns]
    
    if 'Subform_id' not in common_columns:
        print("Error: 'Subform_id' column is missing from one or both datasets.")
        return pd.DataFrame()

    gsheets_df.loc[:, 'Subform_id'] = gsheets_df['Subform_id'].astype(str).str.strip()
    zoho_df.loc[:, 'Subform_id'] = zoho_df['Subform_id'].astype(str).str.strip()
    
    merged_df = zoho_df.merge(gsheets_df, on='Subform_id', how='left', indicator=True, suffixes=('_zoho', '_gsheets'))
    
    missing_rows = merged_df[merged_df['_merge'] == 'left_only'].copy()

    zoho_columns = [col for col in missing_rows.columns if not col.endswith('_gsheets') and col != '_merge']
    missing_rows = missing_rows[zoho_columns]

    missing_rows.columns = [col[:-5] if col.endswith('_zoho') else col for col in missing_rows.columns]

    print(f"Identified {len(missing_rows)} missing rows.")
    return missing_rows


# Step 5: Clear and append data to Google Sheets
def clear_and_append_to_gsheets(sheet_name, worksheet_name, df, json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)

    worksheet.clear()

    data = [df.columns.values.tolist()] + df.values.tolist()

    worksheet.append_rows(data, value_input_option='RAW')
    print(f"Replaced all rows in Google Sheets with {len(df)} rows.")

# Step 6: Sort and append data to Google Sheets
def sort_and_append_to_gsheets(gsheets_df, missing_rows_df, sheet_name, worksheet_name, json_credentials_path):
    combined_df = pd.concat([gsheets_df, missing_rows_df], ignore_index=True)

    combined_df['Date of Order Received'] = pd.to_datetime(combined_df['Date of Order Received'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    combined_df = combined_df.dropna(subset=['Date of Order Received'])

    sorted_df = combined_df.sort_values(by='Date of Order Received', ascending=True)

    new_order =['Subform_id','Lead Zoho ID','version_id','Product_id','Product Name','Version Sheet.Stage','Raptor Invoice','Date of Order Received','Shipping Country','Inco Term','Raptor SKU',
                   'Grainger SKU','Grainger/Non_Grainger','Rpt_Billing_Entity_supplier','ECCN','HS_code','COO'
            
            ]
    sorted_df = sorted_df[new_order]

    sorted_df['Date of Order Received'] = sorted_df['Date of Order Received'].astype(str)

    clear_and_append_to_gsheets(sheet_name, worksheet_name, sorted_df, json_credentials_path)

# Main script execution
if __name__ == "__main__":
    # Fetch data from Zoho
    zoho_df = fetch_data_from_zoho()

    # Fetch data from Google Sheets
    gsheets_df = fetch_data_from_gsheets('Grainger / MCM invoice products', 'Grainger', 'divine-arcade-406611-e0729e40870d.json')

    # Identify missing rows 
    missing_rows_df = identify_missing_rows(zoho_df, gsheets_df)

    if not missing_rows_df.empty:
        # Get delivery dates for missing rows
        missing_rows_df = get_delivery_dates(missing_rows_df, CLIENT_ID, CLIENT_SECRET, client_key, client_secret)

        # Convert and clean the 'Delivery Date' column in the missing_rows_df DataFrame
        missing_rows_df['Delivery Date'] = missing_rows_df['Delivery Date'].apply(convert_dates)

        # Sort, combine, and replace all rows in Google Sheets
        sort_and_append_to_gsheets(gsheets_df, missing_rows_df, 'Grainger / MCM invoice products', 'Grainger', 'divine-arcade-406611-e0729e40870d.json')

        print("Replaced all rows in Google Sheets with sorted and updated data.")
    else:
        print("No missing rows to append.")
