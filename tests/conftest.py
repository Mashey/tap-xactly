# pylint: disable=redefined-outer-name

import os
import dotenv
import pytest
from tap_xactly.client import XactlyClient

dotenv.load_dotenv()


@pytest.fixture
def config():
    return {
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "client_id": os.getenv("CLIENT_ID"),
        "consumer": os.getenv("CONSUMER"),
    }


@pytest.fixture
def state():
    return {}


@pytest.fixture
def client(config):
    client = XactlyClient(config)
    client.setup_connection()
    return client
