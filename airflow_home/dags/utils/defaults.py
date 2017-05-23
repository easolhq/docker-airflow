import datetime


def config_default_args():
    """Default args for Clickstream DAGs."""
    now = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    start_date = now.replace(minute=0, second=0, microsecond=0)
    retry_delay = datetime.timedelta(minutes=5)

    args = {
        'owner': 'astronomer',
        'depends_on_past': False,
        'start_date': start_date,
        'email': 'greg@astronomer.io',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': retry_delay,
        'app_id': None,
        'copy_table': None
    }
    return args
