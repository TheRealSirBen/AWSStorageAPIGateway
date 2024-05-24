import os

from dotenv import load_dotenv
from logging import basicConfig
from logging import INFO
from sys import stdout
from os.path import exists
from os import environ
from os import makedirs

basicConfig(stream=stdout, level=INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def start_app():
    # When dev config file is available
    if exists('.env_dev'):
        load_dotenv('.env_dev')

    load_dotenv()

    #
    temp_dir = environ.get('TEMP_DIR')
    makedirs(temp_dir, exist_ok=True)
