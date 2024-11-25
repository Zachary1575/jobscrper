from celery import Celery
"""
CELERY APP CONFIGURATION
"""
app = Celery('tasks', broker='pyamqp://guest@localhost//', backend='rpc://')

app.conf.update(
    worker_hijack_root_logger=False,
    broker_connection_retry_on_startup=False,
    worker_redirect_stdouts=False
)