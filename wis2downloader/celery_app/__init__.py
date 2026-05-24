
from celery import Celery
from wis2downloader.utils.config import CONFIG

use_celery = CONFIG.get('use_celery', False)
app = None

# Ensure that the Celery app is initialized only if use_celery is True
if use_celery:
    celery_config = CONFIG.get('celery_config', {})
    print(f"Initializing Celery app with configuration {celery_config}")
    broker = celery_config.get('broker_url')
    backend = celery_config.get('result_backend')

    # Check if the required configuration keys are present
    if not broker or not backend:
        raise ValueError("broker_url and result_backend must be set in the celery_config section of your configuration.")

    # Define the broker and result backend URLs
    app = Celery(
        'wis2downloader',
        broker=broker,
        result_backend=backend,
        include=['wis2downloader.tasks']
    )

    # Optional configuration
    app.conf.update(
        task_serializer=celery_config.get('task_serializer', 'json'),
        result_serializer=celery_config.get('result_serializer', 'json'),
        accept_content=celery_config.get('accept_content', ['json']),
        task_time_limit=celery_config.get('task_time_limit', 300),
        task_soft_time_limit=celery_config.get('task_soft_time_limit', 240),
        timezone= celery_config.get('timezone', 'UTC'),
        enable_utc= celery_config.get('enable_utc', True),
        worker_concurrency= celery_config.get('worker_concurrency', 4),
        task_ignore_result=True,  # Don't store task results in Redis
        result_expires=3600,  # Expire results after 1 hour (backup safety)
    )
    
    print("Celery app initialized successfully.")