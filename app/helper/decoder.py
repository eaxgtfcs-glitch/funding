import os
import secrets
import string
from base64 import b64decode, b64encode

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv('KEY')
IV = os.getenv('IV')
alphabet = string.ascii_letters + string.digits


def encrypt_str(input_str, key_base64, iv_base64):
    key = b64decode(key_base64)
    iv = b64decode(iv_base64)

    data = input_str.encode('utf-8')
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    encryptor = cipher.encryptor()

    padding_length = 16 - (len(data) % 16)
    data += bytes([padding_length]) * padding_length

    encrypted_data = encryptor.update(data) + encryptor.finalize()
    encrypted_base64 = b64encode(encrypted_data).decode('utf-8')

    return encrypted_base64


def decrypt_str(input_base64, key_base64, iv_base64):
    key = b64decode(key_base64)
    iv = b64decode(iv_base64)

    encrypted_data = b64decode(input_base64)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()

    plain_text = decryptor.update(encrypted_data) + decryptor.finalize()

    padding_length = plain_text[-1]
    plain_text = plain_text[:-padding_length]

    return plain_text.decode('utf-8')


def create_random_secret():
    key = b64encode(os.urandom(32)).decode('utf-8')  # Генерация случайного ключа длиной 32 байт
    iv = b64encode(os.urandom(16)).decode('utf-8')  # Генерация случайного iv длиной 16 байт
    print(key)  # засунь этот key в файл .env
    print(iv)  # засунь этот iv в файл .env


def encrypt(string: str):
    return encrypt_str(string, KEY, IV)


def decrypt(string: str):
    return decrypt_str(string, KEY, IV)


def generate_password(length: int = 10):
    return ''.join(secrets.choice(alphabet) for _ in range(length))


if __name__ == '__main__':
    create_random_secret()
