from celery import Celery
from celery.schedules import crontab


"""
CELERY APP CONFIGURATION
"""
app = Celery('tasks', broker='pyamqp://guest@localhost//', backend='rpc://')

app.conf.update(
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=False
)

# Celery Beat Configuration
app.conf.beat_schedule = {
    'run-scheudled-time': {
        'task': 'tasks.run_job_scraper',
        'schedule': crontab(minute='*/10'),  # A pilot run is to run every 10 minutes
    },
}
app.conf.timezone = 'UTC'