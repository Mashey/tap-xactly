import singer
from singer import Transformer

# The client name needs to be filled in here
import tap_xactly.helpers as helpers

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = helpers.get_stream_object(stream, state, config)
            stream_schema = helpers.get_stream_schema(stream)
            stream_metadata = helpers.get_stream_metadata(stream)

            LOGGER.info(f"Staring sync for stream: {tap_stream_id}")

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream_obj.replication_key,
            )

            for record in stream_obj.sync():
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata
                )
                LOGGER.info(f"Writing record: {transformed_record}")
                singer.write_record(
                    tap_stream_id,
                    transformed_record,
                )

                singer.write_bookmark(
                    state,
                    tap_stream_id,
                    stream_obj.replication_key,
                    record[stream_obj.replication_key],
                )

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
