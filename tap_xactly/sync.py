import time
import singer
from singer import Transformer
import tap_xactly.helpers as helpers

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    total_records = []
    stream_rps = []

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            stream_start = time.perf_counter()
            record_count = 0
            stream_obj = helpers.get_stream_object(stream, state, config)

            # tap_stream_id = stream.tap_stream_id
            # replication_key = stream_obj.replication_key
            # stream_schema = stream.schema.to_dict()
            # stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info(f"Starting sync for stream: {helpers.get_stream_id(stream)}")

            state = singer.set_currently_syncing(state, helpers.get_stream_id(stream))
            singer.write_state(state)
            singer.write_schema(
                helpers.get_stream_id(stream),
                helpers.get_stream_schema(stream),
                stream_obj.key_properties,
                stream_obj.replication_key,
            )

            for record in stream_obj.sync():
                transformed_record = transformer.transform(
                    record,
                    helpers.get_stream_schema(stream),
                    helpers.get_stream_metadata(stream),
                )

                singer.write_record(
                    helpers.get_stream_id(stream),
                    transformed_record,
                )
                record_count += 1

                singer.write_bookmark(
                    state,
                    helpers.get_stream_id(stream),
                    stream_obj.replication_key,
                    record[stream_obj.replication_key],
                )

            if record_count == 0:
                LOGGER.info("No records to update bookmark")

            stream_stop = time.perf_counter()

            total_records.append(record_count)
            info, rps = metrics(stream_start, stream_stop, record_count)
            stream_rps.append(rps)

            LOGGER.info(f"{info}")

            singer.write_bookmark(state, helpers.get_stream_id(stream), "metrics", info)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    # overall_rps = average_rps(total_records, stream_rps)
    LOGGER.info(
        f"""
                Total Records: {sum(total_records)}
                Overall RPS: {average_rps(total_records, stream_rps):0.6}
                """
    )
    singer.write_bookmark(
        state,
        "Overall",
        "metrics",
        f"Records: {sum(total_records)} / RPS: {average_rps(total_records, stream_rps):0.6}",
    )
    singer.write_state(state)


def metrics(start: float, end: float, records: int):
    # elapsed_time = end - start
    rps = records / get_elapsed_time(start, end)
    info = f"""
                Stream runtime: {get_elapsed_time(start, end):0.6} seconds
                Records: {records}
                RPS: {rps:0.6}
            """
    return info, rps


def average_rps(records: list, rps_list: list) -> float:
    return sum(rps_list) / len(records)


def get_elapsed_time(start: float, end: float) -> float:
    return end - start
