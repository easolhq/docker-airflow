import json
import logging


def is_dir(key):
    return key.endswith('/')


def build_xcom(path):
    """
    Construct the JSON object that downstream tasks expect from XCom.
    """
    logging.info('Pushing path "{}" to XCom'.format(path))
    return json.dumps({'input': {'key': path}})
