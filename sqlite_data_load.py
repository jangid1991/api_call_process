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

def sqlite_load (df, table_name,Load_Date,database_path):
    Load_Timestamp = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")
    engine = create_engine('sqlite:///' + database_path + '/' + 'sqlite_db.db')
    sqlite_connection = engine.connect()
    table_name=table_name.replace(' ','_')
    table_name=table_name.replace('.','_')
    sqlite_table = table_name
    df.to_sql(sqlite_table, sqlite_connection, if_exists='replace',index=False)
    logging.info(str(Load_Timestamp) + ':SQLite Load Finished')
    logging.info(str(Load_Timestamp) + ':Balancing Process Started')
    source_cnt=df.shape[0]
    logging.info(str(Load_Timestamp) + ":source_cnt:" +  str(source_cnt))
    #print("source_cnt:" +  str(source_cnt))
    table_name=table_name.replace(' ','_')
    table_name=table_name.replace('.','_')
    query="select count(*) from {} where Load_Date='{}';".format(table_name,str(Load_Date))
    rs=sqlite_connection.execute(query)
    Target_Cnt = rs.fetchone()[0]
    logging.info(str(Load_Timestamp) + ":Target_Cnt:" +  str(Target_Cnt))
    #print("Target_Cnt:" +  str(Target_Cnt))
    if source_cnt == Target_Cnt:
        logging.info(str(Load_Timestamp) + ":Balancing Successful")
    else:
        logging.error(str(Load_Timestamp) + ":Balancing Failed")
        exit(1)

def file_processing (x,i,data_path,database_path):
    Load_Date = dt.datetime.today().strftime("%Y-%m-%d")
    Load_Timestamp = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")
    logging.info(str(Load_Timestamp) + ':Separate of data based on County ' + i +' Started')
    table_name ="data_{}".format(i.lower())
    print(table_name)
    #p = os.path.join(data_path + '/' +  "data_{}.csv".format(i.lower()))
    Columns = ['Test_Date','New_Positives','Cumulative_Number_of_Positives','Total_Number_of_Tests_Performed','Cumulative_Number_of_Tests_Performed']
    df2=x[Columns]
    df2.insert(5, 'Load_Date', Load_Date)
    logging.info(str(Load_Timestamp) + ':SQLite Load Started')
    sqlite_load(df2,table_name,Load_Date,database_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_path",
                        dest='log_path',
                        type=str)
    parser.add_argument("--data_path",
                        dest='data_path',
                        type=str)
    parser.add_argument("--database_path",
                        dest='database_path',
                        type=str)                                          
    args=parser.parse_args()
    log_path=args.log_path
    data_path=args.data_path
    database_path=args.database_path    
    Load_Timestamp = dt.datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%f")
    Load_Date = dt.datetime.today().strftime("%Y-%m-%d")
    log_file_name=log_path + '/' + 'sqlite_data_load_' + str(Load_Date) + '.log'
    logging.basicConfig(filename=log_file_name, level=logging.INFO)
    logging.info(str(Load_Timestamp) + ':Data read started')
    header_list = ["sid","id","position","created_at","created_meta","updated_at","updated_meta","meta","Test_Date","County","New_Positives","Cumulative_Number_of_Positives","Total_Number_of_Tests_Performed","Cumulative_Number_of_Tests_Performed"]
    df1=pd.read_csv(data_path +'/' + 'raw_data.csv', sep = '~',names=header_list , header=0)
    df1.to_csv(data_path +'/' + 'data_header.csv', sep = '~' , index=False)
    logging.info(str(Load_Timestamp) + ':Data Loaded with Header Information')
    #file_processing(df1)
    logging.info(str(Load_Timestamp) + ':Multi Thread Processing Started')
    jobs = []
    for i, x in df1.groupby('County'):
        thread = threading.Thread(target=file_processing(x,i,data_path,database_path))
        jobs.append(thread)

    # Start the threads
    for j in jobs:
        j.start()   
    # Ensure all of the threads have finished
    for j in jobs:
        j.join()
        #print(j)

    print ("List processing complete.")

if __name__ == "__main__":
    main()        