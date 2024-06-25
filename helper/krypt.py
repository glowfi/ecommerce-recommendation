import os
from Crypto.Cipher import AES
from hashlib import md5
import base64
from dotenv import find_dotenv, load_dotenv

BLOCK_SIZE = AES.block_size

# Load dotenv
load_dotenv(find_dotenv(".env"))
SECRET_REQ_RES = str(os.getenv("SECRET_REQ_RES"))


def get_aes(s):
    m = md5()
    m.update(s.encode("utf-8"))
    key = m.hexdigest()
    m = md5()
    m.update((s + key).encode("utf-8"))
    iv = m.hexdigest()

    return AES.new(key.encode("utf8"), AES.MODE_CBC, iv.encode("utf8")[:BLOCK_SIZE])


# pkcs5 padding
def pad(byte_array):
    pad_len = BLOCK_SIZE - len(byte_array) % BLOCK_SIZE
    return byte_array + (bytes([pad_len]) * pad_len)


# pkcs5 - unpadding
def unpad(byte_array):
    return byte_array[: -ord(byte_array[-1:])]


def _encrypt(data):
    data = pad(data.encode("UTF-8"))
    aes = get_aes(SECRET_REQ_RES)
    encrypted = aes.encrypt(data)
    return base64.urlsafe_b64encode(encrypted).decode("utf-8")


def _decrypt(edata):
    edata = base64.urlsafe_b64decode(edata)
    aes = get_aes(SECRET_REQ_RES)
    return unpad(aes.decrypt(edata)).decode("utf-8")
