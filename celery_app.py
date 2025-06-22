"""Celery application configuration."""

from celery import Celery

# Create Celery app
app = Celery('dealdriver_hubspot')

# Load configuration from config module
app.config_from_object('config.celery_config')

# Auto-discover tasks
app.autodiscover_tasks(['src.tasks'])

# Make app available at module level
__all__ = ['app']