# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import XcPosRelTypeHist, STREAMS


@pytest.fixture
def xc_pos_rel_type_hist_obj(client, state):
    return XcPosRelTypeHist(client, state)


def test_xc_pos_rel_type_hist(xc_pos_rel_type_hist_obj):
    assert xc_pos_rel_type_hist_obj.table_name == "xc_pos_rel_type_hist"
    assert xc_pos_rel_type_hist_obj.tap_stream_id == "xc_pos_rel_type_hist"
    assert xc_pos_rel_type_hist_obj.key_properties == ["POS_REL_TYPE_ID"]
    assert xc_pos_rel_type_hist_obj.object_type == "XC_POS_REL_TYPE_HIST"
    assert xc_pos_rel_type_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_rel_type_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_rel_type_hist" in STREAMS
    assert STREAMS["xc_pos_rel_type_hist"] == XcPosRelTypeHist
