from singer import metadata
from tap_xactly.streams import STREAMS


def get_stream_object(stream, state, config):
    return STREAMS[stream.tap_stream_id](state, config)


def get_stream_id(stream):
    return stream.tap_stream_id


def get_stream_schema(stream) -> dict:
    return stream.schema.to_dict()


def get_stream_metadata(stream):
    return metadata.to_map(stream.metadata)
