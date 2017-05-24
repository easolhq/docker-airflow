"""
Clickstream config.
"""


class ClickstreamActivity(object):
    """Config for clickstream event classes."""

    def __init__(self, workflow_id, table_name, redshift_host, redshift_port, redshift_db, redshift_user,
                 redshift_password, redshift_schema, temp_bucket, name_ver):
        """Initialize clickstream event params."""
        self.workflow_id = workflow_id
        self.table_name = table_name
        self.redshift_host = redshift_host
        self.redshift_port = redshift_port
        self.redshift_db = redshift_db
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_schema = redshift_schema
        self.temp_bucket = temp_bucket

        name, version = name_ver.split(':', 1)
        name, version = 'aries-activity-aries-base', '0.1'
        self.name = name
        self.version = version

    @property
    def task_id(self):
        """Get clickstream task id."""
        return 's3_clickstream_table_copy_{}'.format(self.table_name)

    def serialize(self):
        """Generate config as a nested dict."""
        activity = {
            'task_id': self.task_id,
            'name': self.name,
            'version': self.version,
            'config': {
                'appId': self.workflow_id,
                'table': self.table_name,
                'redshift_host': self.redshift_host,
                'redshift_port': self.redshift_port,
                'redshift_db': self.redshift_db,
                'redshift_user': self.redshift_user,
                'redshift_password': self.redshift_password,
                'redshift_schema': self.redshift_schema,
                'temp_bucket': self.temp_bucket,
                'timedelta': 0
            }
        }
        return activity
