"""
TODO
"""

import boa


def build_dag_id(workflow):
    """
    TODO
    """
    workflow_id = workflow['_id']
    workflow_name = 'clickstream_to_redshift'
    dag_id = '{name}__{id}'.format(id=workflow_id, name=workflow_name)
    return dag_id
