import sys
import time
from art import *
from loguru import logger
import multiprocessing

from celery.apps.beat import Beat

# Custom Libs
from celery_app import app
from global_state import global_instance
from bootstrap_init.rabbitmq import verifyRabbitMQ
from bootstrap_init.initalize import initBootstrap
from bootstrap_init.chrome_options import setChromeOptions

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
> You have to run an instance of the celery worker in order for RabbitMQ pings from celery to work.
  Also have to run the scheduler which is celery beat.
  This can be done by running (where tasks is tasks.py):
  'celery -A tasks worker --loglevel=INFO' -> Worker
  'celery -A celery_app beat --loglevel=info' -> Scheduler
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
    setChromeOptions()
    logger.info("Verifying RabbitMQ Server is live...")
    verifyRabbitMQ("127.0.0.1", 5672) # Host and Port might change in Kubernetes
    logger.info("Starting Celery Scheduler Thread! Executing Job Scraper!")
    scheduler_process = multiprocessing.Process(target=start_celery_beat, name="CeleryBeat", daemon=True)
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
    """Start Celery Worker."""
    app.worker_main(['worker', '--loglevel=info'])

def start_celery_beat():
    """Start Celery Beat programmatically."""
    beat = Beat(app=app)
    beat.run()

def verify_worker(worker_process, logger):
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

