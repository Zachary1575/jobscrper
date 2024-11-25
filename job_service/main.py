import sys
import time
from art import *
from loguru import logger
import multiprocessing

from celery.apps.beat import Beat
from celery.schedules import crontab

# Custom Libs
from celery_app import app
from global_state import global_instance
from global_state import GlobalClass
from bootstrap_init.rabbitmq import verifyRabbitMQ
from bootstrap_init.initalize import initBootstrap
from bootstrap_init.mongo import fetchUniqueCompanyList

"""
--LOGGER CONFIG--
"""
logger.remove()
logger.add(
    sink=sys.stdout, 
    format="<green>{time}</green> | <level>{level}</level> | <level>{message}</level>"
)
global_instance.modify_data("logger", logger)

"""
Some starting up comments:
> You may have to run an instance of the celery worker in order for RabbitMQ pings from celery to work.
  Also have to run the scheduler which is celery beat. However, I should have done this programmatically
  This can be done by running (where tasks is tasks.py):
  'celery -A tasks worker --loglevel=INFO' -> Worker
  'celery -A celery_app beat --loglevel=info' -> Scheduler

> Amongst Seperate Processes, there is no "Global" instance unless using shared memory explicitly.
"""
def main() -> None:
    """
    Bootstrap function.

    @Parameters: 
    N/A

    @Return: 
    None
    """
    Art = text2art("JobScrper")
    print(Art)

    logger.info("Starting Celery Worker Child Process...")
    worker_process = multiprocessing.Process(target=start_celery_worker, name="CeleryWorker", daemon=True)
    worker_process.start()
    logger.info("Finished!")
    
    verify_worker(worker_process, logger)

    initBootstrap()
    logger.info("Initializing Dependencies and Connections...")
    logger.info("Setting Chrome options for Selenium...")
    company_list = fetchUniqueCompanyList()
    logger.info("Verifying RabbitMQ Server is live...")
    verifyRabbitMQ("127.0.0.1", 5672) # Host and Port might change in Kubernetes
    logger.info("Serializing Global Instance & Starting Celery Scheduler Thread!")
    scheduler_process = multiprocessing.Process(target=start_celery_beat, args=(company_list,), name="CeleryBeat", daemon=True)
    scheduler_process.start()
    logger.info("Finished!")

    try:
        logger.info("Main program is running. Press Ctrl + C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Terminating Celery Worker process...")
        worker_process.terminate()
        worker_process.join()
        logger.info("Celery Worker process terminated.")

        logger.info("Terminating Celery Beat process...")
        scheduler_process.terminate()
        scheduler_process.join()
        logger.info("Celery Beat process terminated.")

        logger.info("Exiting Main Program!")

def start_celery_worker():
    """
    Start Celery Worker.
    """
    app.worker_main(['worker', '--loglevel=info'])

def start_celery_beat(company_list):
    """
    Start Celery Beat programmatically. 
    """
    beat_global_instance = GlobalClass("beat")
    beat_global_instance.modify_data("company_urls", company_list)
    global_instance_serialized = beat_global_instance.to_json()
    app.conf.beat_schedule = {
        'run-scheudled-time': {
            'task': 'tasks.run_job_scraper',
            'schedule': crontab(minute='*/1'),  # A pilot run is to run every 10 minutes
            'args': [global_instance_serialized],
        },
    }
    app.conf.timezone = 'UTC'

    beat = Beat(app=app)
    beat.run()

def verify_worker(worker_process, logger):
    """
    DOC STRING
    """
    logger.info("Verifying worker status with retries...")
    retries = 0
    max_retries = 10

    while retries < max_retries:
        try:
            response = app.control.ping(timeout=1)
            if response:
                logger.success("Celery Worker is active:", response)
                return
            else:
                logger.warning("No active worker found. Retrying...")
        except Exception as e:
            logger.error(f"Error during verification (attempt {retries + 1}): {e}")
            logger.error("Terminating Worker Process...")
            worker_process.terminate()
            worker_process.join()
            logger.error("Done.")
            raise Exception(f"Unable to verify Celery Worker: {e}")

        retries += 1
        backoff = min(2 ** retries, 30)  # Exponential backoff, capped at 30 seconds
        time.sleep(backoff)

    print("Failed to verify worker status after multiple attempts.")

if (__name__ == '__main__'):
    main()

