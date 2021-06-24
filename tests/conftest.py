# pylint: disable=redefined-outer-name

import os
import dotenv
import pytest
from tap_xactly.client import XactlyClient
from tap_xactly.discovery import discover

dotenv.load_dotenv()


@pytest.fixture
def config():
    return {
        "user": os.getenv("XACTLY_USER"),
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


@pytest.fixture
def catalog():
    return discover()
