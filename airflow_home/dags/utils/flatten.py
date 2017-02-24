"""Utility methods for flattening nested objects

Available functions:
- flatten_config: flattens config.connection.details onto
connection and config.connection.vpnConnection.details onto vpnConnection
"""
from copy import deepcopy


def remove_fields(obj, fields):
    """returns a copy of obj excluding keys in fields
    Args:
        obj: a dictionary
        fields: keys to remove
    Returns:
        copy of obj excluding fields
    """
    copy = deepcopy(obj)
    for field in fields:
        if field in copy:
            del copy[field]
    return copy


def flatten_connection(obj, key):
    """flattens each value in obj[key] onto obj itself
    Args:
        obj: a dictionary
        key: a string that exists in obj
    Returns:
        A copy of obj with obj[key] flattened onto obj
    """
    copy = deepcopy(obj)
    val = copy.get(key)
    if isinstance(val, dict):
        for k in val:
            copy[k] = val[k]
        del copy[key]
    return copy


def flatten_config(config):
    """flattens connection.details and vpnConnection.details
    Args:
        config: config object from an activity in an activityList
    Returns:
        A copy of config with the connection fields flattened onto the copy
    """
    copy = deepcopy(config)
    # ignore mongoose generated fields
    ignore_fields = ['updatedAt', 'createdAt', '__v', '_id']
    if 'connection' in copy:
        connection = flatten_connection(copy.get('connection'), 'details')
        if 'vpnConnection' in connection:
            vpnConnection = flatten_connection(connection.get('vpnConnection'), 'details')
            connection['vpnConnection'] = remove_fields(vpnConnection, ignore_fields)
        copy['connection'] = remove_fields(connection, ignore_fields)
    return copy
