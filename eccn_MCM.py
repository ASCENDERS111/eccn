import json
import gspread  # type: ignore
import pandas as pd  # type: ignore
import re
import numpy as np  # type: ignore
import requests
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore

# Load credentials from credentials.json
with open('credentials.json') as f:
    credentials = json.load(f)

# Extract credentials   
zoho_params = credentials.get('zoho_params')

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

    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Report_for_ECCN_Mcmaster_Products?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"

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
        df['grkey']=df['Raptor Invoice'].str.split('|').str[0]+df['Grainger SKU']
        df[['Remarks','CRM Update Status']]=''

        # Reorder columns as needed
        ordered_columns = ['Invoice_ID','Subform_id', 'Lead Zoho ID', 'version_id', 'Product_id','SNO', 'Product Name', 'Version Sheet.Stage', 'Raptor Invoice','grkey', 'Date of Order Received', 'Shipping Country', 'Inco Term', 'Raptor SKU',
                           'Grainger SKU', 'Grainger/Non_Grainger', 'Rpt_Billing_Entity_supplier', 'ECCN', 'HS_code', 'COO''Remarks','CRM Update Status'
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

def sort_and_append_to_gsheets(zoho_df, gsheets_df, sheet_name, worksheet_name, json_credentials_path):
    # Ensure 'Subform_id' is treated as string in both dataframes
    zoho_df['Subform_id'] = zoho_df['Subform_id'].astype(str)
    gsheets_df['Subform_id'] = gsheets_df['Subform_id'].astype(str)

    # Debugging: Check original Zoho data before date conversion
    print("Original Zoho Data (first 5 rows):")
    print(zoho_df[['Subform_id', 'Date of Order Received']].head())

    # Convert 'Date of Order Received' in zoho_df (which is in 'dd/mm/yyyy') to datetime using the correct format
    zoho_df['Date of Order Received'] = pd.to_datetime(zoho_df['Date of Order Received'], format='%d/%m/%Y', errors='coerce')

    # Convert 'Date of Order Received' in gsheets_df (assuming 'dd/mm/yyyy' format) to datetime
    gsheets_df['Date of Order Received'] = pd.to_datetime(gsheets_df['Date of Order Received'], format='%d/%m/%Y', errors='coerce')

    # Debugging: Check Zoho data after date conversion
    print("Zoho Data After Conversion (first 5 rows):")
    print(zoho_df[['Subform_id', 'Date of Order Received']].head())

    # Perform a left join
    merged_df = gsheets_df.merge(zoho_df, on='Subform_id', how='left', suffixes=('', '_zoho'))

    # Update columns from Zoho data where they exist
    for col in zoho_df.columns:
        if col in merged_df.columns and col + '_zoho' in merged_df.columns:
            if col == 'Date of Order Received':
                merged_df[col] = merged_df[col].combine_first(merged_df[col + '_zoho'])
            else:
                merged_df[col] = merged_df[col].fillna(merged_df[col + '_zoho'])
        elif col not in merged_df.columns and col + '_zoho' in merged_df.columns:
            merged_df[col] = merged_df[col + '_zoho']

    # Drop the '_zoho' columns
    merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_zoho')]]

    # Identify completely new rows (rows in zoho_df that are not in gsheets_df)
    new_rows = zoho_df[~zoho_df['Subform_id'].isin(gsheets_df['Subform_id'])]

    # Debugging: Check if new rows have the Date of Order Received before appending
    print("New Rows Before Appending (first 5 rows):")
    print(new_rows[['Subform_id', 'Date of Order Received']].head())

    # Append new rows to merged_df
    merged_df = pd.concat([merged_df, new_rows], ignore_index=True)

    # Sort the dataframe by 'Date of Order Received' in descending order
    sorted_df = merged_df.sort_values(by=['Date of Order Received', 'Raptor Invoice'], 
                                    ascending=[False, True], na_position='last')

    # Convert 'Date of Order Received' back to 'dd/mm/yyyy' format
    sorted_df['Date of Order Received'] = sorted_df['Date of Order Received'].dt.strftime('%d/%m/%Y')

    # Replace NaT and NaN with an empty string
    sorted_df = sorted_df.replace({'NaT': '', 'NaN': '', np.nan: ''})

    # Reorder columns as needed
    new_order = ['Invoice_ID','Subform_id', 'Lead Zoho ID', 'version_id', 'Product_id', 'SNO','Product Name', 'Version Sheet.Stage', 'Raptor Invoice','grkey', 'Date of Order Received', 'Shipping Country', 'Inco Term', 'Raptor SKU',
                 'Grainger SKU', 'Grainger/Non_Grainger', 'Rpt_Billing_Entity_supplier', 'ECCN', 'HS_code', 'COO','Remarks','CRM Update Status']
    sorted_df = sorted_df[new_order]

    # Replace all rows in Google Sheets with the updated and new data
    append_to_gsheets(sheet_name, worksheet_name, sorted_df, json_credentials_path, append_only=False)

def append_to_gsheets(sheet_name, worksheet_name, df, json_credentials_path, append_only=False):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)

    # Check if DataFrame is empty before appending
    if df.empty:
        print("No data to update. The DataFrame is empty.")
        return

    # Open the Google Sheet
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)

    # Convert DataFrame to list of lists
    data = [df.columns.values.tolist()] + df.values.tolist()

    # Ensure all values are strings to avoid JSON serialization issues
    data = [[str(cell) if cell is not None else '' for cell in row] for row in data]

    # Clear the worksheet and append all data
    worksheet.clear()
    worksheet.append_rows(data, value_input_option='RAW')
    print(f"Updated Google Sheets with {len(df)} rows of data.")

# Main script execution
if __name__ == "__main__":
    # Fetch data from Zoho
    zoho_df = fetch_data_from_zoho()

    # Fetch data from Google Sheets
    gsheets_df = fetch_data_from_gsheets('Grainger / MCM invoice products', 'MCM_3', 'divine-arcade-406611-e0729e40870d.json')

    # Sort, combine, and append all rows (Zoho + Google Sheets) to Google Sheets
    sort_and_append_to_gsheets(zoho_df, gsheets_df, 'Grainger / MCM invoice products', 'MCM_3', 'divine-arcade-406611-e0729e40870d.json')