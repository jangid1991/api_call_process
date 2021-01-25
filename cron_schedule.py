from crontab import CronTab
cron = CronTab(tab='')
job  = cron.new(command = 'python ./python_executor.py --scripts_path "C:/Users/jjang/Documents/github/api_call_process/" --log_path "C:/Users/jjang/Documents/github/api_call_process/logs" --data_path "C:/Users/jjang/Documents/github/api_call_process/data" --database_path "C:/Users/jjang/Documents/github/api_call_process/database"')
job.hour.every(24)
job.hour.also.on(21)
cron.write(filename='cronTab.tab')