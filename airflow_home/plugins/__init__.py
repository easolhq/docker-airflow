from airflow.plugins_manager import AirflowPlugin
from plugins import hooks, operators


class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    operators = [
        operators.S3GetKeyOperator,
        operators.S3FileKeySensor,
        operators.S3WildcardKeySensor,
    ]
    hooks = [
        hooks.S3FileHook,
    ]
