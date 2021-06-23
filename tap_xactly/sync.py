import time
import singer
from singer import Transformer

# The client name needs to be filled in here
import tap_xactly.helpers as helpers

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    total_records = []
    stream_rps = []

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            # Metrics variables
            stream_start = time.perf_counter()
            record_count = 0

            # tap_stream_id = stream.tap_stream_id
            stream_obj = helpers.get_stream_object(stream, state, config)
            # stream_schema = helpers.get_stream_schema(stream)
            # stream_metadata = helpers.get_stream_metadata(stream)

            LOGGER.info(f"Staring sync for stream: {stream.tap_stream_id}")

            state = singer.set_currently_syncing(state, stream.tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                stream.tap_stream_id,
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
                LOGGER.info(f"Writing record: {transformed_record}")
                singer.write_record(
                    stream.tap_stream_id,
                    transformed_record,
                )
                record_count += 1

                singer.write_bookmark(
                    state,
                    stream.tap_stream_id,
                    stream_obj.replication_key,
                    record[stream_obj.replication_key],
                )

            if record_count == 0:
                LOGGER.info("No Records to update bookmarks.")

            stream_stop = time.perf_counter()

            total_records.append(record_count)
            info, rps = metrics(stream_start, stream_stop, record_count)
            stream_rps.append(rps)
            LOGGER.info(f"{info}")
            singer.write_bookmark(state, stream.tap_stream_id, "metrics", info)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    # overall_rps = overall_metrics(total_records, stream_rps)
    LOGGER.info(
        f"""
                Total Records: {sum(total_records)}
                Overall RPS: {overall_metrics(total_records, stream_rps):0.6}
                """
    )
    singer.write_bookmark(
        state,
        "Overall",
        "metrics",
        f"Records: {sum(total_records)} / RPS: {overall_metrics(total_records, stream_rps):0.6}",
    )
    singer.write_state(state)


def metrics(start: float, end: float, records: int):
    info = f"""
            Stream runtime: {get_elapsed_time(end, start):0.6} seconds
            Records: {records}
            RPS: {average_rps(records, get_elapsed_time(end, start)):0.6}
            """
    return info, average_rps(records, get_elapsed_time(end, start))


def overall_metrics(records: list, rps_list: list) -> float:
    return sum(rps_list) / len(records)


def get_elapsed_time(end: float, start: float) -> float:
    return end - start


def average_rps(records: int, elapsed_time) -> float:
    return records / elapsed_time
