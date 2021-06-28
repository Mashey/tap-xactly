# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import (
    XcPosRelTypeHist,
    XcPosRelations,
    XcPosRelationsHist,
    XcPosTitleAssignment,
    XcPosTitleAssignmentHist,
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
def xc_pos_title_assignment_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosTitleAssignmentHist.tap_stream_id)
    return XcPosTitleAssignmentHist(client, state, stream)


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


def test_xc_pos_relations_hist(xc_pos_relations_hist_obj):
    assert xc_pos_relations_hist_obj.tap_stream_id == "xc_pos_relations_hist"
    assert xc_pos_relations_hist_obj.key_properties == ["ID"]
    assert xc_pos_relations_hist_obj.object_type == "XC_POS_RELATION_HIST"
    assert xc_pos_relations_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_relations_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_relations_hist" in STREAMS
    assert STREAMS["xc_pos_relations_hist"] == XcPosRelationsHist


def test_xc_pos_title_assignment(xc_pos_title_assignment_obj):
    assert xc_pos_title_assignment_obj.tap_stream_id == "xc_pos_title_assignment"
    assert xc_pos_title_assignment_obj.key_properties == ["POS_TITLE_ASSIGNMENT_ID"]
    assert xc_pos_title_assignment_obj.object_type == "XC_POS_TITLE_ASSIGNMENT"
    assert xc_pos_title_assignment_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_title_assignment_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_title_assignment" in STREAMS


def test_xc_pos_title_assignment_hist(xc_pos_title_assignment_hist_obj):
    assert (
        xc_pos_title_assignment_hist_obj.tap_stream_id == "xc_pos_title_assignment_hist"
    )
    assert xc_pos_title_assignment_hist_obj.key_properties == [
        "POS_TITLE_ASSIGNMENT_ID"
    ]
    assert (
        xc_pos_title_assignment_hist_obj.object_type == "XC_POS_TITLE_ASSIGNMENT_HIST"
    )
    assert xc_pos_title_assignment_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_title_assignment_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_title_assignment_hist" in STREAMS
    assert STREAMS["xc_pos_title_assignment_hist"] == XcPosTitleAssignmentHist


def test_xc_attainment_measure(xc_attainment_measure_obj):
    assert xc_attainment_measure_obj.tap_stream_id == "xc_attainment_measure"
    assert xc_attainment_measure_obj.key_properties == ["ATTAINMENT_MEASURE_ID"]
    assert xc_attainment_measure_obj.object_type == "XC_ATTAINMENT_MEASURE"
    assert xc_attainment_measure_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_attainment_measure_obj.replication_key == "MODIFIED_DATE"

    assert "xc_attainment_measure" in STREAMS
    assert STREAMS["xc_attainment_measure"] == XcAttainmentMeasure


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


def test_xc_credit(xc_credit_obj):
    assert xc_credit_obj.tap_stream_id == "xc_credit"
    assert xc_credit_obj.key_properties == ["CREDIT_ID"]
    assert xc_credit_obj.object_type == "XC_CREDIT"
    assert xc_credit_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit" in STREAMS
    assert STREAMS["xc_credit"] == XcCredit
