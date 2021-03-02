from cryptography.fernet import Fernet
from config import Config

key = Config.get('web', 'encryption_key')
f = Fernet(key.encode())

# Encrypt the string 'value' passed in.
def encrypt(value):
    # needs to be a byte string to encrypt, which is why we use .encode()
    encrypted = f.encrypt(value.encode())

    # abaco needs regular strings (not byte strings) so we .decode() back to
    # a regular string
    encrypted = encrypted.decode("utf-8")

    return encrypted


# Decrypt the encrypted 'value' passed in.
def decrypt(value):
    # needs to be a byte string to decrypt, which is why we use .encode()
    decrypted = f.decrypt(value.encode())

    # abaco needs regular strings (not byte strings) so we .decode() back
    decrypted = decrypted.decode("utf-8")

    return decrypted