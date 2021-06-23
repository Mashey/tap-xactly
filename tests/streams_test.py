# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import XcAttainmentMeasure, XcPosRelTypeHist, STREAMS


@pytest.fixture
def xc_pos_rel_type_hist_obj(client, state):
    return XcPosRelTypeHist(client, state)


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


@pytest.fixture
def xc_attainment_measure_obj(client, state):
    return XcAttainmentMeasure(client, state)


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
