def build_dag_id(workflow):
    workflow_id = workflow['_id']
    workflow_name = workflow.get('name', 'astronomer_clickstream_to_redshift')
    workflow_name = stringcase.snakecase(stringcase.lowercase(workflow_name))
    dag_id = '{name}__etl__{id}'.format(id=workflow_id, name=workflow_name)
    return dag_id
