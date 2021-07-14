import time
import singer
from singer import Transformer
from tap_xactly.streams import STREAMS
from tap_xactly.client import XactlyClient


LOGGER = singer.get_logger()


def sync(config, state, catalog):
    sync_metrics = {"total_records": [], "stream_rps": []}
    client = XactlyClient(config)
    client.setup_connection()

    with Transformer() as transformer:
        for catalog_stream in catalog.get_selected_streams(state):
            # Metrics variables
            stream_start = time.perf_counter()
            record_count = 0

            stream = STREAMS[catalog_stream.tap_stream_id](
                client, state, catalog_stream
            )

            LOGGER.info(f"Staring sync for stream: {stream.tap_stream_id}")

            state = singer.set_currently_syncing(state, stream.tap_stream_id)
            singer.write_state(state)
            singer.write_schema(
                stream.tap_stream_id,
                stream.schema,
                stream.key_properties,
                stream.replication_key,
            )

            for record in stream.sync():
                transformed_record = transformer.transform(
                    record,
                    stream.schema,
                    stream.metadata,
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
                    stream.replication_key,
                    record[stream.replication_key],
                )

            if record_count == 0:
                LOGGER.info("No Records to update bookmarks.")

            stream_stop = time.perf_counter()

            sync_metrics["total_records"].append(record_count)
            info, rps = metrics(stream_start, stream_stop, record_count)
            sync_metrics["stream_rps"].append(rps)
            LOGGER.info(f"{info}")
            singer.write_bookmark(state, stream.tap_stream_id, "metrics", info)
            singer.write_state(state)

    client.close_connection()

    state = singer.set_currently_syncing(state, None)
    LOGGER.info(
        f"""
        Total Records: {sum(sync_metrics["total_records"])}
        Overall RPS: {overall_metrics(sync_metrics["total_records"], sync_metrics["stream_rps"])}
        """
    )
    singer.write_bookmark(
        state,
        "Overall",
        "metrics",
        f"""
        Records: {sum(sync_metrics['total_records'])}
        RPS: {overall_metrics(sync_metrics['total_records'], sync_metrics['stream_rps'])}
        """,
    )
    singer.write_state(state)


def metrics(start: float, end: float, records: int):
    info = f"""
            Stream runtime: {get_elapsed_time(end, start)} seconds
            Records: {records}
            RPS: {average_rps(records, get_elapsed_time(end, start))}
            """
    return info, average_rps(records, get_elapsed_time(end, start))


def overall_metrics(records: list, rps_list: list) -> float:
    return sum(rps_list) / len(records)


def get_elapsed_time(end: float, start: float) -> float:
    return end - start


def average_rps(records: int, elapsed_time: float) -> float:
    return records / elapsed_time
