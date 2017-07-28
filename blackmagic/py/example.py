from __future__ import print_function

import os

import blackmagic

passphrase = os.environ['PASSPHRASE']

obj = {
    'my_key_1': 'my_val_1',
    'my_key_2': 'my_val_2',
}
print('obj =', obj)

encrypted_obj = blackmagic.encrypt(passphrase, obj)
if not encrypted_obj:
    print('encryption failed for', obj)
else:
    print('encrypted_obj =', encrypted_obj)

decrypted_obj = blackmagic.decrypt(passphrase, encrypted_obj)
if not decrypted_obj:
    print('decryption failed for', obj)
else:
    print('decrypted_obj =', decrypted_obj)
