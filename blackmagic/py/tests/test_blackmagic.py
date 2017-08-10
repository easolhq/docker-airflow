import os

from blackmagic import (
    build_node_cmd,
    decrypt,
    encrypt,
    kwargs2str,
    parse_json,
    run,
    to_json,
)


def test_kwargs2str():
    actual = kwargs2str([
        ('kwarg1', 'val1'),
        ('kwarg2', 'val2'),
    ])
    expected = "--kwarg1='val1' --kwarg2='val2'"
    assert actual == expected


def test_build_node_cmd():
    actual = build_node_cmd(
        command='encrypt',
        passphrase='foo',
        obj='{"k2": "v2", "k1": "v1"}',
    )
    assert actual.startswith('node')
    assert 'blackmagic.js' in actual
    assert '--command=\'encrypt\'' in actual
    assert '--passphrase=\'foo\'' in actual
    assert '--obj=\'{"k2": "v2", "k1": "v1"}\'' in actual


def test_to_json():
    actual = to_json({
        'k1': 'v1',
        'k2': 'v2',
    })
    assert actual.startswith('{')
    assert actual.endswith('}')
    assert '"k1": "v1"' in actual
    assert '"k2": "v2"' in actual


def test_parse_json():
    actual = parse_json('{"k1": "v1", "k2": "v2"}')
    expected = {
        'k1': 'v1',
        'k2': 'v2',
    }
    assert actual == expected


def test_run():
    actual = run('echo "hello blackmagic"')
    expected = "hello blackmagic"
    assert actual == expected


def test_encrypt():
    actual = encrypt('mypass', {
        'k1': 'v1',
        'k2': 'v2',
    })
    assert 'k1' in actual
    assert 'k2' in actual
    assert actual['k1'] != 'v1'
    assert actual['k2'] != 'v2'


def test_decrypt():
    obj = {
        'k1': 'v1',
        'k2': 'v2',
    }
    encrypted_obj = encrypt('mypass', obj)
    decrypted_obj = decrypt('mypass', encrypted_obj)
    assert 'k1' in decrypted_obj
    assert 'k2' in decrypted_obj
    assert obj['k1'] == decrypted_obj['k1']
    assert obj['k2'] == decrypted_obj['k2']
