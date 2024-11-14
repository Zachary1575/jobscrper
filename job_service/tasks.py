from celery_app import app
from core.workday import coreWorkday

@app.task
def dummy_task():
    return 'RabbitMQ is running on this port!'

@app.task
def dummy_timed_task():
    return "Timed task finished!"

@app.task
def run_job_scraper() -> None:
    """
    DOC STRING
    """
    coreWorkday() # Run Workday Scraper
    return
