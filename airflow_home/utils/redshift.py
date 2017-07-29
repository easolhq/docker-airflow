"""
Redshift / Clickstream DAG utils.
"""

import boa


def get_name(workflow):
    return workflow.get('name', 'astronomer_clickstream_to_redshift')


def func_reduce(*funcs, initial=None):
    """Reduce a chain of nested function calls.
    
    foo(bar(baz(initial))) --> reducec(initial, foo, bar, baz)
    """
    pass  # TODO: implement with reduce()


def build_dag_id(workflow):
    """Build dag_id for Clickstream DAGs."""
    return '{name}__etl__{id}'.format(
        id=workflow['_id'],
        name=func_reduce(get_name(workflow),
                         str.lower,
                         boa.constrict))
