"""Celery configuration settings."""

import os

# Broker settings
broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
result_backend = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

# Serialization
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'

# Timezone
timezone = 'UTC'
enable_utc = True

# Task routing
task_routes = {
    'src.tasks.celery_tasks.scrape_domain': {'queue': 'scraping'},
    'src.tasks.celery_tasks.enrich_company': {'queue': 'enrichment'},
    'src.tasks.celery_tasks.enrich_leads': {'queue': 'enrichment'},
    'src.tasks.celery_tasks.export_company_csv': {'queue': 'export'},
    'src.tasks.celery_tasks.export_leads_csv': {'queue': 'export'},
}

# Worker configuration
worker_prefetch_multiplier = 1
worker_max_tasks_per_child = 100

# Task time limits
task_time_limit = 300  # 5 minutes hard limit
task_soft_time_limit = 240  # 4 minutes soft limit

# Queue configuration
from kombu import Queue

task_queues = (
    Queue('scraping', routing_key='scraping'),
    Queue('enrichment', routing_key='enrichment'),
    Queue('export', routing_key='export'),
)