# Earthquake data API calls

# Pull earthquake data from the Japan Meteorological Agency
#   - List of recent quakes - https://www.jma.go.jp/bosai/quake/data/list.json
#   - Drill down into each event's json to get each event's location including: latitude and longitude
#       - Example: https://www.jma.go.jp/bosai/quake/data/20250903060513_20250903060214_VXSE5k_1.json

import requests
import pandas as pd
import time

def get_earthquake_events():

    # Request the list of recent quake events
    df_recent_events = pd.DataFrame()
    recent_data_url = "https://www.jma.go.jp/bosai/quake/data/list.json"
    
    # Send request
    response = requests.get(recent_data_url)
    if response.status_code == 200:
        recent_events_json = response.json()
        df_recent_events = pd.DataFrame(recent_events_json)

    # Parse list of events
    events_list = df_recent_events["json"]

    # DataFrame for all detailed reports
    df_detailed_events = pd.DataFrame()

    # Request JSON details for each quake event
    for event in events_list:
    
        # Generate url for each event
        url = "https://www.jma.go.jp/bosai/quake/data/" + event

        # Send request
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            temp_df = pd.json_normalize(response_json)
            df_detailed_events = pd.concat([df_detailed_events, temp_df], ignore_index=True)

    # Drop problematic columns before saving to parquet - serialization issue
    for col in ["Head.Headline.Information", "Head.Headline.Information.Item"]:
        if col in df_detailed_events.columns:
            df_detailed_events = df_detailed_events.drop([col], axis=1)

    # Generate timestamp for filename
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    
    # Create a parquet file locally
    df_detailed_events.to_parquet(f'data/raw/quake-{timestamp}.parquet', index=False)