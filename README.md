This repository has python code to read data from API and load the data into individual county table in sqlite db.

Scripts -

1. Api_call_data_load.py - This scripts call the API and load the raw data in csv format.
2. sqlite_data_load.py - This scripts read the files created by first scripts and load into into individual county table in sqlite db.
3. python_executor.py - This is a executor scripts to execute the above mentioned scripts in order.

-- Command To run

[Please change the path "C:/Users/jjang/Documents/github/"  to where you download this repository]


python ./python_executor.py --scripts_path "C:/Users/jjang/Documents/github/api_call_process/" --log_path "C:/Users/jjang/Documents/github/api_call_process/logs" --data_path "C:/Users/jjang/Documents/github/api_call_process/data" --database_path "C:/Users/jjang/Documents/github/api_call_process/database"