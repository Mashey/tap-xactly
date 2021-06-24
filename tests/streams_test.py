# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import (
    XcPosRelTypeHist,
    XcPosRelations,
    XcPosRelationsHist,
    XcPosTitleAssignment,
    XcAttainmentMeasure,
    XcAttainmentMeasureCriteria,
    XcCredit,
    STREAMS,
)


@pytest.fixture
def xc_pos_rel_type_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosRelTypeHist.tap_stream_id)
    return XcPosRelTypeHist(client, state, stream)


@pytest.fixture
def xc_pos_relations_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosRelations.tap_stream_id)
    return XcPosRelations(client, state, stream)


@pytest.fixture
def xc_pos_relations_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosRelationsHist.tap_stream_id)
    return XcPosRelationsHist(client, state, stream)


@pytest.fixture
def xc_pos_title_assignment_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosTitleAssignment.tap_stream_id)
    return XcPosTitleAssignment(client, state, stream)


@pytest.fixture
def xc_attainment_measure_obj(client, state, catalog):
    stream = catalog.get_stream(XcAttainmentMeasure.tap_stream_id)
    return XcAttainmentMeasure(client, state, stream)


@pytest.fixture
def xc_attainment_measure_criteria_obj(client, state, catalog):
    stream = catalog.get_stream(XcAttainmentMeasureCriteria.tap_stream_id)
    return XcAttainmentMeasureCriteria(client, state, stream)


@pytest.fixture
def xc_credit_obj(client, state, catalog):
    stream = catalog.get_stream(XcCredit.tap_stream_id)
    return XcCredit(client, state, stream)


def test_xc_pos_rel_type_hist(xc_pos_rel_type_hist_obj):
    assert xc_pos_rel_type_hist_obj.tap_stream_id == "xc_pos_rel_type_hist"
    assert xc_pos_rel_type_hist_obj.key_properties == ["POS_REL_TYPE_ID"]
    assert xc_pos_rel_type_hist_obj.object_type == "XC_POS_REL_TYPE_HIST"
    assert xc_pos_rel_type_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_rel_type_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_rel_type_hist" in STREAMS
    assert STREAMS["xc_pos_rel_type_hist"] == XcPosRelTypeHist

    records = list(xc_pos_rel_type_hist_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "POS_REL_TYPE_ID" in record
        assert "OBJECT_ID" in record
        assert "VERSION" in record
        assert "NAME" in record
        assert "DESCR" in record
        assert "IS_ACTIVE" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "CREATED_BY_NAME" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "MODIFIED_BY_NAME" in record
        assert "EFFECTIVE_START_DATE" in record
        assert "EFFECTIVE_END_DATE" in record
        assert "IS_MASTER" in record


def test_xc_pos_relations(xc_pos_relations_obj):
    assert xc_pos_relations_obj.tap_stream_id == "xc_pos_relations"
    assert xc_pos_relations_obj.key_properties == ["ID"]
    assert xc_pos_relations_obj.object_type == "XC_POS_RELATION"
    assert xc_pos_relations_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_relations_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_relations" in STREAMS
    assert STREAMS["xc_pos_relations"] == XcPosRelations

    records = list(xc_pos_relations_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "ID" in record
        assert "VERSION" in record
        assert "FROM_POS_ID" in record
        assert "TO_POS_ID" in record
        assert "POS_REL_TYPE_ID" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "CREATED_BY_NAME" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "MODIFIED_BY_NAME" in record
        assert "FROM_POS_NAME" in record
        assert "TO_POS_NAME" in record


def test_xc_pos_relations_hist(xc_pos_relations_hist_obj):
    assert xc_pos_relations_hist_obj.tap_stream_id == "xc_pos_relations_hist"
    assert xc_pos_relations_hist_obj.key_properties == ["ID"]
    assert xc_pos_relations_hist_obj.object_type == "XC_POS_RELATION_HIST"
    assert xc_pos_relations_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_relations_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_relations_hist" in STREAMS
    assert STREAMS["xc_pos_relations_hist"] == XcPosRelationsHist

    records = list(xc_pos_relations_hist_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "ID" in record
        assert "VERSION" in record
        assert "FROM_POS_ID" in record
        assert "TO_POS_ID" in record
        assert "POS_REL_TYPE_ID" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "CREATED_BY_NAME" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "MODIFIED_BY_NAME" in record
        assert "FROM_POS_NAME" in record
        assert "TO_POS_NAME" in record


def test_xc_pos_title_assignment(xc_pos_title_assignment_obj):
    assert xc_pos_title_assignment_obj.tap_stream_id == "xc_pos_title_assignment"
    assert xc_pos_title_assignment_obj.key_properties == ["POS_TITLE_ASSIGNMENT_ID"]
    assert xc_pos_title_assignment_obj.object_type == "XC_POS_TITLE_ASSIGNMENT"
    assert xc_pos_title_assignment_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_title_assignment_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_title_assignment" in STREAMS
    assert STREAMS["xc_pos_title_assignment"] == XcPosTitleAssignment

    records = list(xc_pos_title_assignment_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "POS_TITLE_ASSIGNMENT_ID" in record
        assert "VERSION" in record
        assert "TITLE_ID" in record
        assert "TITLE_NAME" in record
        assert "POSITION_ID" in record
        assert "POSITION_NAME" in record
        assert "IS_ACTIVE" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "CREATED_BY_NAME" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "MODIFIED_BY_NAME" in record


def test_xc_attainment_measure(xc_attainment_measure_obj):
    assert xc_attainment_measure_obj.tap_stream_id == "xc_attainment_measure"
    assert xc_attainment_measure_obj.key_properties == ["ATTAINMENT_MEASURE_ID"]
    assert xc_attainment_measure_obj.object_type == "XC_ATTAINMENT_MEASURE"
    assert xc_attainment_measure_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_attainment_measure_obj.replication_key == "MODIFIED_DATE"

    assert "xc_attainment_measure" in STREAMS
    assert STREAMS["xc_attainment_measure"] == XcAttainmentMeasure

    records = list(xc_attainment_measure_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "ATTAINMENT_MEASURE_ID" in record
        assert "NAME" in record
        assert "DESCRIPTION" in record
        assert "PERIOD_TYPE" in record
        assert "IS_ACTIVE" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "EFFECTIVE_START_PERIOD_ID" in record
        assert "EFFECTIVE_END_PERIOD_ID" in record
        assert "MASTER_ATTAINMENT_MEASURE_ID" in record
        assert "VERSION" in record
        assert "HISTORY_UUID" in record


def test_xc_attainment_measure_criteria(xc_attainment_measure_criteria_obj):
    assert (
        xc_attainment_measure_criteria_obj.tap_stream_id
        == "xc_attainment_measure_criteria"
    )
    assert xc_attainment_measure_criteria_obj.key_properties == [
        "ATTAINMENT_MEASURE_CRITERIA_ID"
    ]
    assert (
        xc_attainment_measure_criteria_obj.object_type
        == "XC_ATTAINMENT_MEASURE_CRITERIA"
    )
    assert xc_attainment_measure_criteria_obj.valid_replication_keys == [
        "MODIFIED_DATE"
    ]
    assert xc_attainment_measure_criteria_obj.replication_key == "MODIFIED_DATE"

    assert "xc_attainment_measure_criteria" in STREAMS
    assert STREAMS["xc_attainment_measure_criteria"] == XcAttainmentMeasureCriteria

    records = list(xc_attainment_measure_criteria_obj.sync())
    assert len(records) > 0

    for record in records:
        assert "ATTAINMENT_MEASURE_CRITERIA_ID" in record
        assert "CRITERIA_ID" in record
        assert "ATTAINMENT_MEASURE_ID" in record
        assert "TYPE" in record
        assert "NAME" in record
        assert "IS_ACTIVE" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "HISTORY_UUID" in record


def test_xc_credit(xc_credit_obj):  # pylint: disable=too-many-statements
    assert xc_credit_obj.tap_stream_id == "xc_credit"
    assert xc_credit_obj.key_properties == ["CREDIT_ID"]
    assert xc_credit_obj.object_type == "XC_CREDIT"
    assert xc_credit_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit" in STREAMS
    assert STREAMS["xc_credit"] == XcCredit

    records = xc_credit_obj.client.query_database(
        table_name="xc_credit",
        primary_key="credit_id",
        limit=5,
        offset=0,
        limit_key="MODIFIED_DATE",
        limit_key_value="2021-05-21T00:52:18",
    )
    assert len(records) > 0

    for record in records:
        assert "CREDIT_ID" in record
        assert "VERSION" in record
        assert "NAME" in record
        assert "IS_ACTIVE" in record
        assert "PERIOD_ID" in record
        assert "PERIOD_NAME" in record
        assert "AMOUNT" in record
        assert "AMOUNT_UNIT_TYPE_ID" in record
        assert "AMOUNT_DISPLAY_SYMBOL" in record
        assert "PARTICIPANT_ID" in record
        assert "PARTICIPANT_NAME" in record
        assert "POSITION_ID" in record
        assert "POSITION_NAME" in record
        assert "ORDER_ITEM_ID" in record
        assert "ITEM_CODE" in record
        assert "ORDER_CODE" in record
        assert "RULE_ID" in record
        assert "RULE_NAME" in record
        assert "IS_PROCESSED" in record
        assert "IS_ROLLABLE" in record
        assert "SOURCE_CREDIT_ID" in record
        assert "SRC_POS_RELATION_TYPE_ID" in record
        assert "CREDIT_TYPE_ID" in record
        assert "CREDIT_TYPE_NAME" in record
        assert "IS_HELD" in record
        assert "RELEASE_DATE" in record
        assert "REASON_CODE_ID" in record
        assert "REASON_CODE_NAME" in record
        assert "ROLLABLE_ON_REPORTING" in record
        assert "PRODUCT_ID" in record
        assert "PRODUCT_NAME" in record
        assert "CUSTOMER_ID" in record
        assert "CUSTOMER_NAME" in record
        assert "GEOGRAPHY_ID" in record
        assert "GEOGRAPHY_NAME" in record
        assert "TRANS_ID" in record
        assert "BATCH_NUMBER" in record
        assert "SUB_BATCH_NUMBER" in record
        assert "EVER_ON_HOLD" in record
        assert "RELEASE_GROUP_ID" in record
        assert "ESTIMATED_REL_DATE" in record
        assert "BUSINESS_GROUP_ID" in record
        assert "RUN_ID" in record
        assert "INCENTIVE_DATE" in record
        assert "EFF_PARTICIPANT_ID" in record
        assert "EFF_POSITION_ID" in record
        assert "PLAN_ID" in record
        assert "PLAN_NAME" in record
        assert "SOURCE_POSITION_ID" in record
        assert "CREATED_DATE" in record
        assert "CREATED_BY_ID" in record
        assert "CREATED_BY_NAME" in record
        assert "MODIFIED_DATE" in record
        assert "MODIFIED_BY_ID" in record
        assert "MODIFIED_BY_NAME" in record
        assert "SUB_PART_KEY" in record
        assert "MGR_EFF_POS_ID" in record
        assert "MGR_MASTER_POS_ID" in record
        assert "MGR_EFF_PART_ID" in record
