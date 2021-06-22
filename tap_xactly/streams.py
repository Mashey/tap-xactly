from typing import List
import singer

from tap_xactly.client import XactlyClient

LOGGER = singer.get_logger()


class Stream:
    tap_stream_id = ""
    key_properties: List[str] = [""]
    replication_method = ""
    valid_replication_keys = [""]
    replication_key = "last_updated_at"
    object_type = ""
    limit = 1000
    selected = True

    def __init__(self, client: XactlyClient, state: dict):
        self.client = client
        self.state = state

    def sync(self):
        bookmark_value = singer.get_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            "1970-01-11T00:00:01Z",
        )
        offset = 0
        last_query_record_count = self.limit
        while last_query_record_count >= self.limit:
            try:
                record_count = 0
                records = self.client.query_database(
                    self.tap_stream_id,
                    limit=self.limit,
                    offset=offset,
                    primary_key=self.key_properties[0],
                    limit_key=self.replication_key,
                    limit_key_value=bookmark_value,
                )
                for record in records:
                    record_count += 1
                    yield record

                offset += self.limit + 1
                last_query_record_count = record_count

            except Exception as ex:
                LOGGER.warning(f"Client error {ex} :: Closing SQL and Connection.")
                self.client.close_connection()
                LOGGER.info("Restarting Client")
                self.client.setup_connection()
                continue

        self.client.close_connection()

        LOGGER.info(f"{self.tap_stream_id} sync completed.")
        LOGGER.info(f"Creating bookmark for {self.tap_stream_id} stream")


class IncrementalStream(Stream):
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):
    replication_method = "FULL_TABLE"


class XcPosRelTypeHist(IncrementalStream):
    tap_stream_id = "xc_pos_rel_type_hist"
    key_properties = ["POS_REL_TYPE_ID"]
    object_type = "XC_POS_REL_TYPE_HIST"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


STREAMS = {
    "xc_pos_rel_type_hist": XcPosRelTypeHist,
}
