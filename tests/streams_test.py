# pylint: disable=redefined-outer-name
import pytest
from tap_xactly.streams import (
    XcPosHierarchy,
    XcPosHierarchyHist,
    XcPosHierarchyType,
    XcPosHierarchyTypeHist,
    XcPosPartAssignment,
    XcPosPartAssignmentHist,
    XcPosRelType,
    XcPosRelTypeHist,
    XcPosRelations,
    XcPosRelationsHist,
    XcPosition,
    XcPositionHist,
    XcPosTitleAssignment,
    XcPosTitleAssignmentHist,
    XcAttainmentMeasure,
    XcAttainmentMeasureCriteria,
    XcCredit,
    XcCreditAdjustment,
    XcCreditHeld,
    XcQuota,
    XcQuotaHist,
    XcQuotaAssignment,
    XcQuotaAssignmentHist,
    XcQuotaRelationship,
    XcQuotaTotals,
    XcCreditTotals,
    XcCreditType,
    XcRole,
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
def xc_position_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosition.tap_stream_id)
    return XcPosition(client, state, stream)


@pytest.fixture
def xc_position_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPositionHist.tap_stream_id)
    return XcPositionHist(client, state, stream)


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


@pytest.fixture
def xc_credit_adjustment_obj(client, state, catalog):
    stream = catalog.get_stream(XcCreditAdjustment.tap_stream_id)
    return XcCreditAdjustment(client, state, stream)


@pytest.fixture
def xc_credit_held_obj(client, state, catalog):
    stream = catalog.get_stream(XcCreditHeld.tap_stream_id)
    return XcCreditHeld(client, state, stream)


@pytest.fixture
def xc_credit_totals_obj(client, state, catalog):
    stream = catalog.get_stream(XcCreditTotals.tap_stream_id)
    return XcCreditTotals(client, state, stream)


@pytest.fixture
def xc_quota_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuota.tap_stream_id)
    return XcQuota(client, state, stream)


@pytest.fixture
def xc_quota_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuotaHist.tap_stream_id)
    return XcQuotaHist(client, state, stream)


@pytest.fixture
def xc_credit_type_obj(client, state, catalog):
    stream = catalog.get_stream(XcCreditType.tap_stream_id)
    return XcCreditType(client, state, stream)


@pytest.fixture
def xc_quota_assignment_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuotaAssignment.tap_stream_id)
    return XcQuotaAssignment(client, state, stream)


@pytest.fixture
def xc_quota_assignment_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuotaAssignmentHist.tap_stream_id)
    return XcQuotaAssignmentHist(client, state, stream)


@pytest.fixture
def xc_quota_relationship_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuotaRelationship.tap_stream_id)
    return XcQuotaRelationship(client, state, stream)


@pytest.fixture
def xc_quota_totals_obj(client, state, catalog):
    stream = catalog.get_stream(XcQuotaTotals.tap_stream_id)
    return XcQuotaTotals(client, state, stream)


@pytest.fixture
def xc_pos_hierarchy_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosHierarchy.tap_stream_id)
    return XcPosHierarchy(client, state, stream)


@pytest.fixture
def xc_pos_hierarchy_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosHierarchyHist.tap_stream_id)
    return XcPosHierarchyHist(client, state, stream)


@pytest.fixture
def xc_pos_hierarchy_type_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosHierarchyType.tap_stream_id)
    return XcPosHierarchyType(client, state, stream)


@pytest.fixture
def xc_pos_hierarchy_type_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosHierarchyTypeHist.tap_stream_id)
    return XcPosHierarchyTypeHist(client, state, stream)


@pytest.fixture
def xc_pos_part_assignment_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosPartAssignment.tap_stream_id)
    return XcPosPartAssignment(client, state, stream)


@pytest.fixture
def xc_pos_part_assignment_hist_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosPartAssignmentHist.tap_stream_id)
    return XcPosPartAssignmentHist(client, state, stream)


@pytest.fixture
def xc_pos_rel_type_obj(client, state, catalog):
    stream = catalog.get_stream(XcPosRelType.tap_stream_id)
    return XcPosRelType(client, state, stream)


@pytest.fixture
def xc_role_obj(client, state, catalog):
    stream = catalog.get_stream(XcRole.tap_stream_id)
    return XcRole(client, state, stream)


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
    assert STREAMS["xc_pos_title_assignment"] == XcPosTitleAssignment


def test_xc_position(xc_position_obj):
    assert xc_position_obj.tap_stream_id == "xc_position"
    assert xc_position_obj.key_properties == ["POSITION_ID"]
    assert xc_position_obj.object_type == "XC_POSITION"
    assert xc_position_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_position_obj.replication_key == "MODIFIED_DATE"

    assert "xc_position" in STREAMS
    assert STREAMS["xc_position"] == XcPosition


def test_xc_position_hist(xc_position_hist_obj):
    assert xc_position_hist_obj.tap_stream_id == "xc_position_hist"
    assert xc_position_hist_obj.key_properties == ["POSITION_ID"]
    assert xc_position_hist_obj.object_type == "XC_POSITION_HIST"
    assert xc_position_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_position_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_position" in STREAMS
    assert STREAMS["xc_position"] == XcPosition


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


def test_xc_credit_adjustment(
    xc_credit_adjustment_obj,
):
    assert xc_credit_adjustment_obj.tap_stream_id == "xc_credit_adjustment"
    assert xc_credit_adjustment_obj.key_properties == ["CREDIT_ID"]
    assert xc_credit_adjustment_obj.object_type == "XC_CREDIT_ADJUSTMENT"
    assert xc_credit_adjustment_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_adjustment_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit_adjustment" in STREAMS
    assert STREAMS["xc_credit_adjustment"] == XcCreditAdjustment


def test_xc_credit_held(xc_credit_held_obj):
    assert xc_credit_held_obj.tap_stream_id == "xc_credit_held"
    assert xc_credit_held_obj.key_properties == ["CREDIT_HELD_ID"]
    assert xc_credit_held_obj.object_type == "XC_CREDIT_HELD"
    assert xc_credit_held_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_held_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit_held" in STREAMS
    assert STREAMS["xc_credit_held"] == XcCreditHeld


def test_xc_credit_totals(xc_credit_totals_obj):
    assert xc_credit_totals_obj.tap_stream_id == "xc_credit_totals"
    assert xc_credit_totals_obj.key_properties == ["CREDIT_TOTALS_ID"]
    assert xc_credit_totals_obj.object_type == "XC_CREDIT_TOTALS"
    assert xc_credit_totals_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_totals_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit_totals" in STREAMS
    assert STREAMS["xc_credit_totals"] == XcCreditTotals


def test_xc_quota(xc_quota_obj):
    assert xc_quota_obj.tap_stream_id == "xc_quota"
    assert xc_quota_obj.key_properties == ["QUOTA_ID"]
    assert xc_quota_obj.object_type == "XC_QUOTA"
    assert xc_quota_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota" in STREAMS
    assert STREAMS["xc_quota"] == XcQuota


def test_xc_quota_hist(xc_quota_hist_obj):
    assert xc_quota_hist_obj.tap_stream_id == "xc_quota_hist"
    assert xc_quota_hist_obj.key_properties == ["QUOTA_ID"]
    assert xc_quota_hist_obj.object_type == "XC_QUOTA_HIST"
    assert xc_quota_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota_hist" in STREAMS
    assert STREAMS["xc_quota_hist"] == XcQuotaHist


def test_xc_credit_type(xc_credit_type_obj):
    assert xc_credit_type_obj.tap_stream_id == "xc_credit_type"
    assert xc_credit_type_obj.key_properties == ["CREDIT_TYPE_ID"]
    assert xc_credit_type_obj.object_type == "XC_CREDIT_TYPE"
    assert xc_credit_type_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_credit_type_obj.replication_key == "MODIFIED_DATE"

    assert "xc_credit_type" in STREAMS
    assert STREAMS["xc_credit_type"] == XcCreditType


def test_xc_quota_assignment(xc_quota_assignment_obj):
    assert xc_quota_assignment_obj.tap_stream_id == "xc_quota_assignment"
    assert xc_quota_assignment_obj.key_properties == ["QUOTA_ASSIGNMENT_ID"]
    assert xc_quota_assignment_obj.object_type == "XC_QUOTA_ASSIGNMENT"
    assert xc_quota_assignment_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_assignment_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota_assignment" in STREAMS
    assert STREAMS["xc_quota_assignment"] == XcQuotaAssignment


def test_xc_quota_assignment_hist(xc_quota_assignment_hist_obj):
    assert xc_quota_assignment_hist_obj.tap_stream_id == "xc_quota_assignment_hist"
    assert xc_quota_assignment_hist_obj.key_properties == ["QUOTA_ASSIGNMENT_ID"]
    assert xc_quota_assignment_hist_obj.object_type == "XC_QUOTA_ASSIGNMENT_HIST"
    assert xc_quota_assignment_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_assignment_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota_assignment_hist" in STREAMS
    assert STREAMS["xc_quota_assignment_hist"] == XcQuotaAssignmentHist


def test_xc_quota_relationship(xc_quota_relationship_obj):
    assert xc_quota_relationship_obj.tap_stream_id == "xc_quota_relationship"
    assert xc_quota_relationship_obj.key_properties == ["QUOTA_RELATIONSHIP_ID"]
    assert xc_quota_relationship_obj.object_type == "XC_QUOTA_RELATIONSHIP"
    assert xc_quota_relationship_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_relationship_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota_relationship" in STREAMS
    assert STREAMS["xc_quota_relationship"] == XcQuotaRelationship


def test_xc_quota_totals(xc_quota_totals_obj):
    assert xc_quota_totals_obj.tap_stream_id == "xc_quota_totals"
    assert xc_quota_totals_obj.key_properties == ["QUOTA_TOTALS_ID"]
    assert xc_quota_totals_obj.object_type == "XC_QUOTA_TOTALS"
    assert xc_quota_totals_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_quota_totals_obj.replication_key == "MODIFIED_DATE"

    assert "xc_quota_totals" in STREAMS
    assert STREAMS["xc_quota_totals"] == XcQuotaTotals


def test_xc_pos_hierarchy(xc_pos_hierarchy_obj):
    assert xc_pos_hierarchy_obj.tap_stream_id == "xc_pos_hierarchy"
    assert xc_pos_hierarchy_obj.key_properties == ["POS_HIERARCHY_ID"]
    assert xc_pos_hierarchy_obj.object_type == "XC_POS_HIERARCHY"
    assert xc_pos_hierarchy_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_hierarchy_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_hierarchy" in STREAMS
    assert STREAMS["xc_pos_hierarchy"] == XcPosHierarchy


def test_xc_pos_hierarchy_hist(xc_pos_hierarchy_hist_obj):
    assert xc_pos_hierarchy_hist_obj.tap_stream_id == "xc_pos_hierarchy_hist"
    assert xc_pos_hierarchy_hist_obj.key_properties == ["POS_HIERARCHY_ID"]
    assert xc_pos_hierarchy_hist_obj.object_type == "XC_POS_HIERARCHY_HIST"
    assert xc_pos_hierarchy_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_hierarchy_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_hierarchy_hist" in STREAMS
    assert STREAMS["xc_pos_hierarchy_hist"] == XcPosHierarchyHist


def test_xc_pos_hierarchy_type(xc_pos_hierarchy_type_obj):
    assert xc_pos_hierarchy_type_obj.tap_stream_id == "xc_pos_hierarchy_type"
    assert xc_pos_hierarchy_type_obj.key_properties == ["POS_HIERARCHY_TYPE_ID"]
    assert xc_pos_hierarchy_type_obj.object_type == "XC_POS_HIERARCHY_TYPE"
    assert xc_pos_hierarchy_type_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_hierarchy_type_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_hierarchy_type" in STREAMS
    assert STREAMS["xc_pos_hierarchy_type"] == XcPosHierarchyType


def test_xc_pos_hierarchy_type_hist(xc_pos_hierarchy_type_hist_obj):
    assert xc_pos_hierarchy_type_hist_obj.tap_stream_id == "xc_pos_hierarchy_type_hist"
    assert xc_pos_hierarchy_type_hist_obj.key_properties == ["POS_HIERARCHY_TYPE_ID"]
    assert xc_pos_hierarchy_type_hist_obj.object_type == "XC_POS_HIERARCHY_TYPE_HIST"
    assert xc_pos_hierarchy_type_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_hierarchy_type_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_hierarchy_type_hist" in STREAMS
    assert STREAMS["xc_pos_hierarchy_type_hist"] == XcPosHierarchyTypeHist


def test_xc_pos_part_assignment(xc_pos_part_assignment_obj):
    assert xc_pos_part_assignment_obj.tap_stream_id == "xc_pos_part_assignment"
    assert xc_pos_part_assignment_obj.key_properties == ["POS_PART_ASSIGNMENT_ID"]
    assert xc_pos_part_assignment_obj.object_type == "XC_POS_PART_ASSIGNMENT"
    assert xc_pos_part_assignment_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_part_assignment_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_part_assignment" in STREAMS
    assert STREAMS["xc_pos_part_assignment"] == XcPosPartAssignment


def test_xc_pos_part_assignment_hist(xc_pos_part_assignment_hist_obj):
    assert (
        xc_pos_part_assignment_hist_obj.tap_stream_id == "xc_pos_part_assignment_hist"
    )
    assert xc_pos_part_assignment_hist_obj.key_properties == ["POS_PART_ASSIGNMENT_ID"]
    assert xc_pos_part_assignment_hist_obj.object_type == "XC_POS_PART_ASSIGNMENT_HIST"
    assert xc_pos_part_assignment_hist_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_part_assignment_hist_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_part_assignment_hist" in STREAMS
    assert STREAMS["xc_pos_part_assignment_hist"] == XcPosPartAssignmentHist


def test_xc_pos_rel_type(xc_pos_rel_type_obj):
    assert xc_pos_rel_type_obj.tap_stream_id == "xc_pos_rel_type"
    assert xc_pos_rel_type_obj.key_properties == ["POS_REL_TYPE_ID"]
    assert xc_pos_rel_type_obj.object_type == "XC_POS_REL_TYPE"
    assert xc_pos_rel_type_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_pos_rel_type_obj.replication_key == "MODIFIED_DATE"

    assert "xc_pos_rel_type_hist" in STREAMS
    assert STREAMS["xc_pos_rel_type"] == XcPosRelType


def test_xc_role(xc_role_obj):
    assert xc_role_obj.tap_stream_id == "xc_role"
    assert xc_role_obj.key_properties == ["ROLE_ID"]
    assert xc_role_obj.object_type == "XC_ROLE"
    assert xc_role_obj.valid_replication_keys == ["MODIFIED_DATE"]
    assert xc_role_obj.replication_key == "MODIFIED_DATE"

    assert "xc_role" in STREAMS
    assert STREAMS["xc_role"] == XcRole
