"""Celery application configuration."""

from celery import Celery

# Create Celery app
app = Celery('dealdriver_hubspot')

# Load configuration from config module
app.config_from_object('config.celery_config')

# Ensure result backend is set
app.conf.update(
    result_backend='redis://localhost:6379/0',
    broker_url='redis://localhost:6379/0',
)

# Auto-discover tasks
app.autodiscover_tasks(['src.tasks'])

# Make app available at module level
__all__ = ['app']