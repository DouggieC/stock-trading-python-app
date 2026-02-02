import schedule
import time
from script import run_stock_job
from datetime import datetime

def basic_job():
    print("Job started at:\t", datetime.now())

# Run every minute
schedule.every().minute.do(basic_job)
# Run every minute
schedule.every().hour.do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)
    