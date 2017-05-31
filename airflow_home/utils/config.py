"""
Clickstream config.
"""


class ClickstreamActivity(object):
    """Config for clickstream event classes."""

    def __init__(self, workflow_id, table_name, redshift_host, redshift_port, redshift_db, redshift_user,
                 redshift_password, redshift_encrypted, temp_bucket, name_ver):
        """Initialize clickstream event params."""
        self.workflow_id = workflow_id
        self.table_name = table_name
        self.redshift_host = redshift_host
        self.redshift_port = redshift_port
        self.redshift_db = redshift_db
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_encrypted = redshift_encrypted
        self.temp_bucket = temp_bucket
        self.name, self.version = name_ver.split(':', 1) if name_ver is not None else 'aries-activity-aries-base', '0.1'

        self.required_params = [
            self.workflow_id,
            self.table_name,
            self.redshift_host,
            self.redshift_port,
            self.redshift_db,
            self.redshift_user,
            self.redshift_password,
            self.redshift_encrypted,
            self.temp_bucket,
            self.name,
            self.version
        ]

    @property
    def redshift_schema(self):
        return self.workflow_id

    @property
    def task_id(self):
        """Get clickstream task id."""
        return 's3_clickstream_table_copy_{}'.format(self.table_name)

    def is_valid(self):
        # TODO: handle if values are empty strings
        missing_params = [i for i in self.required_params if i is None]
        return len(missing_params) == 0

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
                'timedelta': 0,
                '_encrypted': self.redshift_encrypted,
            }
        }
        return activity
