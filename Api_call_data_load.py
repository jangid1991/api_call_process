import pandas as pd
import json
import requests
from pandas.io.json import json_normalize
import os
import threading
import datetime as dt 
from sqlalchemy import create_engine
import logging
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_path",
                        dest='log_path',
                        type=str)
    parser.add_argument("--data_path",
                        dest='data_path',
                        type=str)                        
    args=parser.parse_args()
    log_path=args.log_path
    data_path=args.data_path
    Load_Timestamp = dt.datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%f")
    Load_Date = dt.datetime.today().strftime("%Y-%m-%d")
    log_file_name=log_path + '/' + 'Api_call_data_load_' + str(Load_Date) + '.log'
    print(log_file_name)
    logging.basicConfig(filename=log_file_name, level=logging.INFO)
    response = requests.get('https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD')
    response_value = response.status_code
    if response_value == 200:
        print(response)
        logging.info(str(Load_Timestamp) + ':API Response Code-' + str(response) )
    else:
        print(response)
        logging.error(str(Load_Timestamp) + ':API Response Code-' + str(response) + ':API Call Failed')
        exit(1)
    df = pd.json_normalize(response.json() , record_path='data')
    logging.info(str(Load_Timestamp) + ':Data has been extract from API')
    df.rename(columns={'0': 'sid', '1': 'id','2': 'position', '3': 'created_at', '4': 'created_meta', '5': 'updated_at', '6': 'updated_meta', '7': 'meta', '8': 'Test_Date', '9': 'County', '10': 'New_Positives', '11': 'Cumulative_Number_of_Positives', '12': 'Total_Number_of_Tests_Performed', '13': 'Cumulative_Number_of_Tests_Performed' }, inplace=True)
    # Saving to CSV format
    csv_file_name=data_path + '/' + 'raw_data.csv'
    df.to_csv(csv_file_name, sep = '~' , index=False)
    logging.info(str(Load_Timestamp) + ':Data has been Extract in CSV Format')


if __name__ == "__main__":
    main()