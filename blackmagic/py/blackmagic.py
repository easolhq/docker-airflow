"""
Run Node in Python.
"""

import json
import logging
import os
import os.path
from collections import OrderedDict

import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BLACKMAGIC_HOME = os.environ['BLACKMAGIC_HOME']


def kwargs2str(kwargs_list):
    """Construct a string of an ordered list of command-line keyword args."""
    od = OrderedDict(kwargs_list)
    kwargs_str = ' '.join('--{}=\'{}\''.format(k, v) for k, v in od.items())
    return kwargs_str


def build_node_cmd(command, passphrase, obj):
    """Construct the shell command to run our node scripts."""
    cmd_kwargs_str = kwargs2str([
        ('command', command),
        ('passphrase', passphrase),
        ('obj', obj),
    ])
    cmd = 'node {script} {kwargs}'.format(
        script=os.path.join(BLACKMAGIC_HOME, 'js', 'blackmagic.js'),
        kwargs=cmd_kwargs_str,
    )
    return cmd


def to_json(obj):
    """Convert a dict to serialized JSON string."""
    return json.dumps(obj)


def parse_json(str):
    """Convert a serialized JSON string to a dict."""
    return json.loads(str)


def run(cmd):
    """Run a command in a child process."""
    try:
        # rv = subprocess.check_output(cmd, shell=True, universal_newlines=True, timeout=5)
        rv = subprocess.check_output(cmd, shell=True, universal_newlines=True, timeout=10)
    except subprocess.CalledProcessError as e:
        logger.error('child proc error - {}'.format(e))
        return None
    except subprocess.TimeoutExpired as e:
        logger.error('run\'s subprocess took too long', e)
        return None
    return rv.rstrip('\n')


def encrypt(passphrase, obj):
    """Execute a node script that runs cryptobject.encrypt."""
    cmd = build_node_cmd('encrypt', passphrase, to_json(obj))
    rv = run(cmd)
    if not rv:
        return None
    return parse_json(rv)


def decrypt(passphrase, obj):
    """Execute a node script that runs cryptobject.decrypt."""
    cmd = build_node_cmd('decrypt', passphrase, to_json(obj))
    rv = run(cmd)
    if not rv:
        return None
    return parse_json(rv)
