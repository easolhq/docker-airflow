"""
Redshift / Clickstream DAG utils.
"""

import boa


def build_dag_id(workflow):
    """Build dag_id for Clickstream DAGs."""
    workflow_id = workflow['_id']
    workflow_name = workflow.get('name', 'astronomer_clickstream_to_redshift')
    workflow_name = boa.constrict(workflow_name.lower())
    dag_id = '{name}__etl__{id}'.format(id=workflow_id, name=workflow_name)
    return dag_id
