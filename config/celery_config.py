"""Celery configuration settings."""

import os

# Broker settings
BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

# Serialization
TASK_SERIALIZER = 'json'
ACCEPT_CONTENT = ['json']
RESULT_SERIALIZER = 'json'

# Timezone
TIMEZONE = 'UTC'
ENABLE_UTC = True

# Task routing
TASK_ROUTES = {
    'src.tasks.celery_tasks.scrape_domain': {'queue': 'scraping'},
    'src.tasks.celery_tasks.enrich_company': {'queue': 'enrichment'},
    'src.tasks.celery_tasks.enrich_leads': {'queue': 'enrichment'},
    'src.tasks.celery_tasks.export_company_csv': {'queue': 'export'},
    'src.tasks.celery_tasks.export_leads_csv': {'queue': 'export'},
}

# Worker configuration
WORKER_PREFETCH_MULTIPLIER = 1
WORKER_MAX_TASKS_PER_CHILD = 100

# Task time limits
TASK_TIME_LIMIT = 300  # 5 minutes hard limit
TASK_SOFT_TIME_LIMIT = 240  # 4 minutes soft limit

# Queue configuration
CELERY_QUEUES = {
    'scraping': {
        'exchange': 'scraping',
        'routing_key': 'scraping',
    },
    'enrichment': {
        'exchange': 'enrichment',
        'routing_key': 'enrichment',
    },
    'export': {
        'exchange': 'export',
        'routing_key': 'export',
    },
}