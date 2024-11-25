from celery_app import app
from core.workday import coreWorkday
from global_state import GlobalClass

from selenium.webdriver.chrome.options import Options

@app.task
def dummy_task():
    return 'RabbitMQ is running on this port!'

@app.task
def dummy_timed_task():
    return "Timed task finished!"

@app.task
def run_job_scraper(global_instance_serialized):
    """
    DOC STRING
    """
    import sys
    import redis
    import pandas as pd
    from loguru import logger

    # Create a distributed lock
    r = redis.Redis(host='localhost', port=6379, db=1)
    lock_key = "lock:jobscrper_task"
    lock = r.lock(lock_key, timeout=180)  # Lock expires after 60 seconds

    if lock.acquire(blocking=False):  # Non-blocking attempt to acquire the lock
        try:
            logger.remove()
            logger.add(
                sink=sys.stdout, 
                format="<green>{time}</green> | <level>{level}</level> | <level>{message}</level>"
            )
            logger.info("Executing Job Scraper Tasks!")

            process_data = GlobalClass.from_json(global_instance_serialized) # Now its not global, because we will have to pass this payload class for every function

            chrome_options = Options()
            chrome_options.add_argument("--headless=new") # for Chrome >= 109
            prefs = {
            "profile.managed_default_content_settings.images": 2,  # Disable images
            }
            chrome_options.add_experimental_option("prefs", prefs)
            process_data.modify_data("chrome_options", chrome_options)
            logger.success(f"Chrome Options saved in Process Instance!")

            if (process_data.get_data("company_urls") == None):
                logger.error("'company_urls' are not fetched! Aborting!")
                raise Exception("'company_urls' are not fetched! Aborting!")
            
            df = pd.DataFrame(process_data.get_data("company_urls"))
            coreWorkday(df, process_data, logger) # Run Workday Scraper
        finally:
            logger.info("Releasing lock & disconnecting from Redis!")
            lock.release()
            r.close()
    else:
        print("Job Scraper is already running. Skipping this instance.")

    return "'run_job_scraper' finished executing task!"
