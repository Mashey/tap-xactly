# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import (
    XcPosRelTypeHist,
    XcPosRelations,
    XcAttainmentMeasure,
    XcAttainmentMeasureCriteria,
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
def xc_attainment_measure_obj(client, state, catalog):
    stream = catalog.get_stream(XcAttainmentMeasure.tap_stream_id)
    return XcAttainmentMeasure(client, state, stream)


@pytest.fixture
def xc_attainment_measure_criteria_obj(client, state, catalog):
    stream = catalog.get_stream(XcAttainmentMeasureCriteria.tap_stream_id)
    return XcAttainmentMeasureCriteria(client, state, stream)


def test_xc_pos_rel_type_hist(xc_pos_rel_type_hist_obj):
    assert xc_pos_rel_type_hist_obj.tap_stream_id == "xc_pos_rel_type_hist"
    assert xc_pos_rel_type_hist_obj.key_properties == ["POS_REL_TYPE_ID"]
    assert xc_pos_rel_type_hist_obj.object_type == "XC_POS_REL_TYPE_HIST"
    assert xc_pos_rel_type_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_rel_type_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_rel_type_hist" in STREAMS
    assert STREAMS["xc_pos_rel_type_hist"] == XcPosRelTypeHist

    records = list(xc_pos_rel_type_hist_obj.sync())

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


def test_xc_attainment_measure(xc_attainment_measure_obj):
    assert xc_attainment_measure_obj.tap_stream_id == "xc_attainment_measure"
    assert xc_attainment_measure_obj.key_properties == ["ATTAINMENT_MEASURE_ID"]
    assert xc_attainment_measure_obj.object_type == "XC_ATTAINMENT_MEASURE"
    assert xc_attainment_measure_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_attainment_measure_obj.replication_key == "MODIFIED_DATE"

    assert "xc_attainment_measure" in STREAMS
    assert STREAMS["xc_attainment_measure"] == XcAttainmentMeasure

    records = list(xc_attainment_measure_obj.sync())

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
