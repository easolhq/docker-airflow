from utils.redshift import build_dag_id


def test_build_dag_id_with_name():
    workflow = {'_id': '123', 'name': 'foo bar'}
    output = build_dag_id(workflow)
    expected = 'clickstream_to_redshift__123'
    assert output == expected


def test_build_dag_id_without_name():
    workflow = {'_id': '123'}
    output = build_dag_id(workflow)
    expected = 'clickstream_to_redshift__123'
    assert output == expected
