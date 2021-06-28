from typing import List
import singer
from singer import metadata

from tap_xactly.client import XactlyClient

LOGGER = singer.get_logger()


class Stream:  # pylint: disable=too-few-public-methods
    tap_stream_id = ""
    key_properties: List[str] = [""]
    replication_method = ""
    valid_replication_keys = [""]
    replication_key = "last_updated_at"
    object_type = ""
    limit = 1000
    selected = True

    def __init__(self, client: XactlyClient, state: dict, stream):
        self.client = client
        self.state = state
        self.schema = stream.schema.to_dict()
        self.metadata = metadata.to_map(stream.metadata)
        self.bookmark_date = "1970-01-11T00:00:01Z"
        self.last_primary = 0

    def sync(self):
        bookmark = singer.get_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            self.bookmark_date,
        )

        last_query_record_count = self.limit
        while last_query_record_count >= self.limit:
            try:
                record_count = 0
                records = self.client.query_database(
                    table_name=self.tap_stream_id,
                    limit=self.limit,
                    primary_key=self.key_properties[0],
                    bkmrk_primary_key=self.last_primary,
                    replication_key=self.replication_key,
                    bkmrk_date=bookmark,
                )
                for record in records:
                    self.bookmark_date = record["MODIFIED_DATE"]
                    self.last_primary = record[self.key_properties[0]]
                    record_count += 1
                    yield record

                last_query_record_count = record_count

            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.warning(f"Client error {ex} :: Closing SQL and Connection.")
                self.client.close_connection()
                LOGGER.info("Restarting Client")
                self.client.setup_connection()
                continue

        self.client.close_connection()

        LOGGER.info(f"{self.tap_stream_id} sync completed.")
        LOGGER.info(f"Creating bookmark for {self.tap_stream_id} stream")


class IncrementalStream(Stream):  # pylint: disable=too-few-public-methods
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):  # pylint: disable=too-few-public-methods
    replication_method = "FULL_TABLE"


class XcPosRelTypeHist(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_pos_rel_type_hist"
    key_properties = ["POS_REL_TYPE_ID"]
    object_type = "XC_POS_REL_TYPE_HIST"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcPosRelations(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_pos_relations"
    key_properties = ["ID"]
    object_type = "XC_POS_RELATION"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcPosRelationsHist(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_pos_relations_hist"
    key_properties = ["ID"]
    object_type = "XC_POS_RELATION_HIST"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcPosition(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_position"
    key_properties = ["POSITION_ID"]
    object_type = "XC_POSITION"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcPosTitleAssignment(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_pos_title_assignment"
    key_properties = ["POS_TITLE_ASSIGNMENT_ID"]
    object_type = "XC_POS_TITLE_ASSIGNMENT"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcPosTitleAssignmentHist(
    IncrementalStream
):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_pos_title_assignment_hist"
    key_properties = ["POS_TITLE_ASSIGNMENT_ID"]
    object_type = "XC_POS_TITLE_ASSIGNMENT_HIST"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcAttainmentMeasure(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_attainment_measure"
    key_properties = ["ATTAINMENT_MEASURE_ID"]
    object_type = "XC_ATTAINMENT_MEASURE"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcAttainmentMeasureCriteria(
    IncrementalStream
):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_attainment_measure_criteria"
    key_properties = ["ATTAINMENT_MEASURE_CRITERIA_ID"]
    object_type = "XC_ATTAINMENT_MEASURE_CRITERIA"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcCredit(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_credit"
    key_properties = ["CREDIT_ID"]
    object_type = "XC_CREDIT"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcCreditAdjustment(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_credit_adjustment"
    key_properties = ["CREDIT_ID"]
    object_type = "XC_CREDIT_ADJUSTMENT"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcCreditHeld(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_credit_held"
    key_properties = ["CREDIT_HELD_ID"]
    object_type = "XC_CREDIT_HELD"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


class XcCreditTotals(IncrementalStream):  # pylint: disable=too-few-public-methods
    tap_stream_id = "xc_credit_totals"
    key_properties = ["CREDIT_TOTALS_ID"]
    object_type = "XC_CREDIT_TOTALS"
    valid_replication_keys = ["MODIFIED_DATE"]
    replication_key = "MODIFIED_DATE"


STREAMS = {
    "xc_pos_rel_type_hist": XcPosRelTypeHist,
    "xc_pos_relations": XcPosRelations,
    "xc_pos_relations_hist": XcPosRelationsHist,
    "xc_pos_title_assignment": XcPosTitleAssignment,
    "xc_pos_title_assignment_hist": XcPosTitleAssignmentHist,
    "xc_attainment_measure": XcAttainmentMeasure,
    "xc_attainment_measure_criteria": XcAttainmentMeasureCriteria,
    "xc_credit": XcCredit,
    "xc_credit_adjustment": XcCreditAdjustment,
    "xc_credit_held": XcCreditHeld,
    "xc_position": XcPosition,
    "xc_credit_totals": XcCreditTotals,
}
