from typing import List
import singer

LOGGER = singer.get_logger()


class Stream:
    tap_stream_id = ""
    key_properties: List[str] = [""]
    replication_method = ""
    valid_replication_keys = [""]
    replication_key = "last_updated_at"
    object_type = ""
    selected = True

    def __init__(self, client, state):
        self.client = client
        self.state = state

    def sync(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class IncrementalStream(Stream):
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):
    replication_method = "FULL_TABLE"


class XcPosRelTypeHist(IncrementalStream):
    table_name = "xc_pos_rel_type_hist"
    tap_stream_id = "xc_pos_rel_type_hist"
    key_properties = ["POS_REL_TYPE_ID"]
    object_type = "XC_POS_REL_TYPE_HIST"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"

    def sync(self, *args, **kwargs):
        pass


STREAMS = {
    "xc_pos_rel_type_hist": XcPosRelTypeHist,
}
