"""
Test functions for flattening config object.
"""

from dags.utils.flatten import flatten_config


def test_flatten_config():
    """Test flatten_config works for complete config."""
    # arrange
    vpn_connection = {'code': 'vpn_connection_code', 'details': {'remote_host': '192.168.1.0'}}
    connection = {'code': 'connection_code', 'details': {'host': 'example.com'}, 'vpnConnection': vpn_connection}
    config = {'connection': connection}
    # act
    flattened_config = flatten_config(config)
    # assert
    flat_connection = flattened_config.get('connection')
    flat_vpn_connection = flat_connection.get('vpnConnection')
    assert flat_connection.get('details') is None
    assert flat_connection.get('host') == 'example.com'
    assert flat_vpn_connection.get('details') is None
    assert flat_vpn_connection.get('remote_host') == '192.168.1.0'


def test_returns_copy():
    """Test flatten_config makes a copy of passed object."""
    config = {}
    flattened_config = flatten_config(config)
    assert config is not flattened_config


def test_without_connection():
    """Test flatten_config works without connection key in config."""
    config = {'name': 'test_name'}
    flattened_config = flatten_config(config)
    assert config['name'] == 'test_name'


def test_ignores_mongoose_fields():
    """Test flatten_config skips generated fields."""
    connection = {'_id': '12345', 'details': {'host': 'example.com'}, 'updatedAt': '02-25-2017', 'createdAt': '02-25-2017', '__v': 0}
    config = {'connection': connection}
    flattened_config = flatten_config(config)
    assert flattened_config.get('connection').get('_id') is None
    assert flattened_config.get('connection').get('updatedAt') is None
    assert flattened_config.get('connection').get('createdAt') is None
    assert flattened_config.get('connection').get('__v') is None
    assert flattened_config.get('connection').get('host') == 'example.com'
