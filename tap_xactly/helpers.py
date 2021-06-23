from singer import metadata
from tap_xactly.streams import STREAMS
from tap_xactly.client import XactlyClient


def get_stream_object(stream, state, config):
    return STREAMS[stream.tap_stream_id](get_client(config), state)


def get_stream_schema(stream) -> dict:
    return stream.schema.to_dict()


def get_stream_metadata(stream):
    return metadata.to_map(stream.metadata)


def get_client(config):
    return XactlyClient(config)
