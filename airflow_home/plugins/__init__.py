from airflow.plugins_manager import AirflowPlugin
from plugins import hooks, operators, executors


class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    executors = [
        executors.AstronomerMesosExecutor,
    ]
    operators = [
        operators.S3GetKeyOperator,
        operators.S3FileKeySensor,
        operators.S3WildcardKeySensor,
        operators.S3ClickstreamKeySensor,
    ]
    hooks = [
        hooks.S3FileHook,
    ]
