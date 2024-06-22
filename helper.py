import string
from uuid import uuid4
from json import load
from functools import wraps
from logging import info

KEY_SALT_SPACE = ' ' + string.ascii_letters + string.punctuation + string.digits
KEY_SPACE = string.ascii_letters + string.digits
with open('mimetype.json') as mimetype_file:
    MIMETYPE = load(mimetype_file)
    mimetype_file.close()


def log_function_call(func):
    """
    A wrapper function that logs when the program passes through a function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        info("Run function {} start".format(func.__name__))
        result = func(*args, **kwargs)
        info("Run function {} end".format(func.__name__))
        return result

    return wrapper


def generate_temporary_filename(file_extension: str):
    filename = '{}.{}'.format(str(uuid4()), file_extension)
    return filename


def prepare_file_name(filename: str) -> tuple[str, str]:
    file_extension = filename.split('.')[-1]
    return filename, file_extension


def get_file_media_type(file_extension: str) -> str:
    return MIMETYPE.get(file_extension)


def key_position_match(index: int, space_length: int):
    modulo = index % space_length
    if modulo == 0:
        return space_length
    return modulo


def generate_salt(salt_space: str):
    salt = list(salt_space)
    salt_space_length = len(salt)
    salt_index = [KEY_SALT_SPACE.index(_char) for _char in salt]
    salt_position = list(range(1, salt_space_length + 1))

    return salt_space_length, salt_index, salt_position


def generate_cipher(salt_space: str):
    salt_space_length, salt_index, salt_position = generate_salt(salt_space)

    key = list(KEY_SPACE)
    key_space_length = len(key)
    key_position = [key_position_match(index, salt_space_length) for index in range(1, key_space_length + 1)]
    key_arrange = list()
    for position in key_position:
        salt_position_index = salt_position.index(position)
        key_arrange.append(salt_index[salt_position_index])

    cipher = key.copy()
    for _char, arranger in zip(cipher, key_arrange):
        _char_index = cipher.index(_char)
        cipher.pop(_char_index)
        cipher.insert(arranger, _char)

    return key, cipher


def encrypt(original_text: str, encryption_key: str):
    key, cipher = generate_cipher(encryption_key)

    original_text_token = list(original_text)

    encrypted_text_token = list()
    for token in original_text_token:
        key_index = key.index(token)
        cipher_token = cipher[key_index]
        encrypted_text_token.append(cipher_token)

    encrypted_text = ''.join(encrypted_text_token)

    return encrypted_text


def decrypt(encrypted_text: str, encryption_key: str):
    key, cipher = generate_cipher(encryption_key)

    encrypted_text_token = list(encrypted_text)

    original_text_token = list()
    for token in encrypted_text_token:
        cipher_index = cipher.index(token)
        key_token = key[cipher_index]
        original_text_token.append(key_token)

    return ''.join(original_text_token)


def file_or_dir(item: str):
    if item.endswith('/'):
        return 'dir'

    return 'file'
