import json
import logging

is_dir = lambda key: key.endswith('/')


def build_xcom(path):
    """
    Construct the JSON object that downstream tasks expect from XCom.
    """
    logging.info('Pushing path "{}" to XCom'.format(path))
    return json.dumps({'input': {'key': path}})


def parse_keys(keys):
    """Parses input string and returns list of stringified S3 keys"""
    stringified_keys = keys[1:-1]
    activity_key_list = key_splitter(stringified_keys)
    unique_key_list = []
    for key in activity_key_list:
        obj = json.loads(key)
        if 'input' in obj and isinstance(obj['input'], dict):
            unique_key_list.append(obj['input']['key'])
    return unique_key_list


def key_splitter(key_string):
    """Splits input string returning only the dicts as strings"""
    # Split creates list with characters and dicts.
    # Dicts are every odd value in list, hence the [1::2]
    return key_string.split('\'')[1::2]
