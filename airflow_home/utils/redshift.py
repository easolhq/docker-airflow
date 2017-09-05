"""
Redshift / Clickstream DAG utils.
"""

import boa


def build_dag_id(workflow):
    """Build dag_id for Clickstream DAGs."""
    workflow_id = workflow['_id']
    workflow_name = 'clickstream_v2_to_redshift'
    dag_id = '{name}__{id}'.format(id=workflow_id, name=workflow_name)
    return dag_id
