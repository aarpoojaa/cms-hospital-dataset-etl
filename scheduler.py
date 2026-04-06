# Best practice is to have a separate scheduler script that imports the main function from your main script and runs it at the scheduled time. 
# This way, you can keep your scheduling logic separate from your main application logic.
# But, for simplicity, we can directly call the main function from the cms_dataset_etl.py script in our scheduler.py script.

import schedule
import time

def job():
    print("Running scheduled job...")
    main()

schedule.every().day.at("00:00").do(job)

if __name__ == "__main__":
    print("Scheduler started. Waiting for the scheduled time...")
    while True:
        schedule.run_pending()
        time.sleep(60)