from airflow.plugins_manager import AirflowPlugin
from plugins import hooks, operators


class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    operators = [
        operators.AstronomerS3GetKeyAction,
        operators.AstronomerS3KeySensor,
        operators.AstronomerS3WildcardKeySensor,
    ]
    hooks = [
        hooks.S3FileHook,
    ]
