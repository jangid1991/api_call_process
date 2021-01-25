import os
import argparse
import logging
import subprocess

LOGGER =logging.getLogger()
LOGGER.setLevel(logging.INFO)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scripts_path",
                        dest='scripts_path',
                        type=str)
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
    scripts_path=args.scripts_path
    log_path=args.log_path
    data_path=args.data_path
    database_path=args.database_path
    print(scripts_path)
    api_call = subprocess.Popen(f'python ' + scripts_path +'/' + 'Api_call_data_load.py --log_path ' + log_path +  ' --data_path ' +data_path)
    api_call.wait()
    data_call = subprocess.Popen(f'python ' + scripts_path +'/' + 'sqlite_data_load.py --log_path ' + log_path +  ' --data_path ' +data_path +  ' --database_path ' +database_path)
    data_call.wait()

#if __name__ == "__main__":
main()