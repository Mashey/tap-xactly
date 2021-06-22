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
    key_properties = ["ENDPOINT1_id"]
    object_type = "XC_POS_REL_TYPE_HIST"
    valid_replication_keys = ["modified_date"]
    replication_key = "modified_date"

    def sync(self, *args, **kwargs):
        ## This is where to setup iteration over each end point
        # response = self.client.fetch_ENDPOINT1s(ENDPOINT1_PARAMETERS)
        # ENDPOINT1s = response.get('data', {}).get('ENDPOINT1_list', [])
        # for ENDPOINT1 in ENDPOINT1s:
        #   yield ENDPOINT1
        pass


STREAMS = {
    "xc_pos_rel_type_hist": XcPosRelTypeHist,
}
