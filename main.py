# main.py
import schedule
import time
from src.pipeline import run_pipeline

schedule.every(30).minutes.do(run_pipeline)
i=0
while True:
    schedule.run_pending(i)
    time.sleep(1)
    i+=1
    